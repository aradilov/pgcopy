package pgcopy

import (
	"bytes"
	"context"
	"fmt"
	metric "github.com/VictoriaMetrics/metrics"
	"github.com/aradilov/batcher"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

type BatcherSingle struct {
	BatcherConfig

	// metricPrefix is a prefix for metrics collected by the batcher.
	metricPrefix string

	concurrentBatchesOverflow *metric.Counter

	pushRowSuccess  *metric.Counter
	pushRowOverflow *metric.Counter
	pushRowError    *metric.Counter

	pushBatchDuration *metric.Histogram
	pushBatchSize     *metric.Histogram
	pushBatchBytes    *metric.Counter

	pushBatchSuccess *metric.Counter
	pushBatchError   *metric.Counter
	pushBatchRetries *metric.Counter

	concurrentBatchesCh chan struct{}

	eventsBatchers    []*batcher.BytesBatcher
	eventsBatchersIdx uint32

	once sync.Once

	conn                 *pgxpool.Pool
	statementDescription *pgconn.StatementDescription

	quotedTableName   string
	quotedColumnNames string
	totalColumns      int

	m *pgtype.Map

	singleTransactionAnyError  atomic.Value
	singleTransactionErrors    uint32
	singleTransactionWaitGroup sync.WaitGroup
}

func NewBatcherSingle(cfg BatcherConfig) (Batcher, error) {
	bt := &BatcherSingle{BatcherConfig: cfg}
	bt.HideConcurrentInsertBatchError = true
	if err := bt.controlledInitialization(); nil != err {
		return nil, err
	}

	return bt, nil
}

// PushRow pushes new row into batcher.
//
// appendRow must append the new row to b and return new b.
func (cb *BatcherSingle) PushRow(appendRow func(b []byte) []byte) {

	var eb *batcher.BytesBatcher
	if len(cb.eventsBatchers) == 1 {
		eb = cb.eventsBatchers[0]
	} else {
		n := atomic.AddUint32(&cb.eventsBatchersIdx, 1)
		idx := n % uint32(len(cb.eventsBatchers))
		eb = cb.eventsBatchers[idx]
	}

	eb.Push(func(dst []byte, rows int) []byte {
		dst = AppendInt16(dst, int16(cb.totalColumns))
		return appendRow(dst)
	})
}

// GetPushFailure returns the total number of rows failed to be sent
// to DBMS due to various reasons.
func (cb *BatcherSingle) GetPushFailure() uint64 {
	return cb.pushRowOverflow.Get() + cb.pushRowError.Get()
}

func (cb *BatcherSingle) Append(b []byte, pos int, v any) ([]byte, error) {
	// prevent out of range fatal
	if pos >= len(cb.statementDescription.Fields) {
		return b, fmt.Errorf("position %d out of range of len(columns)=%d", pos, len(cb.statementDescription.Fields))
	}

	sp := len(b)
	b = AppendInt32(b, -1)
	argBuf, err := cb.m.Encode(cb.statementDescription.Fields[pos].DataTypeOID, pgtype.BinaryFormatCode, v, b)
	if nil != err {
		return b, err
	}
	if argBuf != nil {
		b = argBuf
		SetInt32(b[sp:], int32(len(b[sp:])-4))
	}

	return b, nil
}

func (cb *BatcherSingle) GetConn() *pgxpool.Pool {
	return cb.conn
}

func (cb *BatcherSingle) Stop() (err error, errorsCount int) {
	err, errorsCount = cb.stop()
	cb.conn.Close()
	return err, errorsCount
}

func (cb *BatcherSingle) controlledInitialization() error {
	cb.initMetrics()

	cb.quotedTableName = sanitize(strings.Split(cb.TableName, "."), ".")

	columns := strings.Split(cb.TableColumns, ",")
	cb.quotedColumnNames = sanitize(columns, ",")
	cb.totalColumns = len(columns)

	cb.m = pgtype.NewMap()

	ctx := context.Background()
	var err error
	if cb.conn, err = pgxpool.New(ctx, cb.ConnAddr); err != nil {
		return fmt.Errorf("failed to open PostgreSQL connection: %v", err)
	}

	if err = cb.conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping PostgreSQL database: %v", err)
	}

	if conn, err := cb.conn.Acquire(ctx); nil != err {
		return fmt.Errorf("failed to get Conn from PostgreSQL Pool: %v", err)
	} else if cb.statementDescription, err = conn.Conn().Prepare(ctx, "", fmt.Sprintf("SELECT %s FROM %s", cb.quotedColumnNames, cb.quotedTableName)); nil != err {
		return fmt.Errorf("failed to get table definition  %v", err)
	} else {
		conn.Release()
	}

	concurrency := runtime.GOMAXPROCS(-1)
	n := cb.MaxConcurrentBatches
	if n == 0 {
		n = concurrency
	}

	cb.concurrentBatchesCh = make(chan struct{}, n)

	for i := 0; i < concurrency; i++ {
		eb := &batcher.BytesBatcher{
			BatchFunc:    cb.concurrentPushBatchToDB,
			MaxBatchSize: cb.MaxBatchSize,
			MaxDelay:     cb.MaxBatchDelay,
		}

		eb.HeaderFunc = func(dst []byte) []byte {
			dst = append(dst[:0], "PGCOPY\n\377\r\n\000"...)
			dst = AppendInt32(dst, 0)
			dst = AppendInt32(dst, 0)
			return dst
		}

		cb.eventsBatchers = append(cb.eventsBatchers, eb)
	}

	return nil
}

func (cb *BatcherSingle) stop() (err error, errorsCount int) {

	for _, b := range cb.eventsBatchers {
		b.Stop()
	}

	// close the channel after making no batch is waiting for the channel
	cb.singleTransactionWaitGroup.Wait()
	close(cb.concurrentBatchesCh)

	av := cb.singleTransactionAnyError.Load()
	if nil == av {
		return nil, 0
	}
	return av.(error), int(atomic.LoadUint32(&cb.singleTransactionErrors))
}

func (cb *BatcherSingle) init() {
	err := cb.controlledInitialization()
	if nil != err {
		log.Fatalf("Error while initializaing BatcherSingle: %v", err)
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
		<-c

		cb.conn.Close()
	}()
}

func (cb *BatcherSingle) initMetrics() {
	cb.concurrentBatchesOverflow = metric.NewCounter(cb.metricPrefix + "ConcurrentBatchesOverflow")

	cb.pushRowSuccess = metric.NewCounter(cb.metricPrefix + "PushRowSuccess")
	cb.pushRowOverflow = metric.NewCounter(cb.metricPrefix + "PushRowOverflow")
	cb.pushRowError = metric.NewCounter(cb.metricPrefix + "PushRowError")

	cb.pushBatchDuration = metric.NewHistogram(cb.metricPrefix + "PushBatchDuration")
	cb.pushBatchSize = metric.NewHistogram(cb.metricPrefix + "PushBatchSize")
	cb.pushBatchBytes = metric.NewCounter(cb.metricPrefix + "PushBatchBytes")

	cb.pushBatchSuccess = metric.NewCounter(cb.metricPrefix + "PushBatchSuccess")
	cb.pushBatchError = metric.NewCounter(cb.metricPrefix + "PushBatchError")
	cb.pushBatchRetries = metric.NewCounter(cb.metricPrefix + "PushBatchRetries")
}

func (cb *BatcherSingle) concurrentPushBatchToDB(sql []byte, itemsCount int) {
	if len(sql) < 19 {
		log.Println("Weird SQL w/o PGCOPY signature: ", string(sql))
		return
	}

	// mark the batch so it doesn't get lost if batcher.Stop is called
	cb.singleTransactionWaitGroup.Add(1)

	// just wait until we have a free slot
	cb.concurrentBatchesCh <- struct{}{}

	sqlCopy := make([]byte, 0, len(sql))
	sqlCopy = append(sqlCopy, sql...)

	go func() {
		cb.pushBatchToDB(sqlCopy, itemsCount)
		cb.singleTransactionWaitGroup.Done()
		<-cb.concurrentBatchesCh
	}()
}

func (cb *BatcherSingle) pushBatchToDB(sql []byte, _ int) {
	err := cb.batchInsert(sql)
	if err == nil {
		return
	}

	atomic.AddUint32(&cb.singleTransactionErrors, 1)
	cb.singleTransactionAnyError.Store(err)
	return
}

func (cb *BatcherSingle) batchInsert(sql []byte) error {
	writeTimeout := cb.WriteTimeout
	if writeTimeout == 0 {
		writeTimeout = defaultWriteTimeout
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), writeTimeout)

	conn, err := cb.conn.Acquire(ctx)
	if nil == err {
		r := bytes.NewReader(sql)
		_, err = conn.Hijack().PgConn().CopyFrom(ctx, r, fmt.Sprintf("copy %s ( %s ) from stdin binary;", cb.quotedTableName, cb.quotedColumnNames))
		conn.Release()
	}
	cancelFunc()

	return err
}
