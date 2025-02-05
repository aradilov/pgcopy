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
	"github.com/valyala/fastrand"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const defaultWriteTimeout = time.Minute

type BatcherConfig struct {
	// DBMS PostgreSQL Connection string
	ConnAddr string

	// Write timeout for COPY command
	WriteTimeout time.Duration

	// MaxRetries is the maximum number of retries before giving up when
	// sending batches to DBMS.
	MaxRetries int

	// Table name
	TableName string

	// List of columns
	TableColumns string

	// MaxConcurrentBatches is the maximum number of batches
	// that may be pushed concurrently to DBMS.
	MaxConcurrentBatches int

	// MaxBatchSize is the maximum size of each batch.
	MaxBatchSize int

	// MaxBatchDelay is the maximum delay before sending batch
	// to DBMS.
	MaxBatchDelay time.Duration

	// MaxConcurrentRescuedBatches is the maximum number of rescued batches
	// that may be pushed concurrently to DBMS.
	MaxConcurrentRescuedBatches *int

	HideConcurrentInsertBatchError bool
}

func NewBatcher(cfg BatcherConfig, rescueDir, metricPrefix string) *Batcher {
	bt := &Batcher{BatcherConfig: cfg}
	bt.rescueDir = rescueDir
	bt.metricPrefix = metricPrefix
	return bt
}

func NewBatcherSingle(cfg BatcherConfig) (*Batcher, error) {
	bt := &Batcher{BatcherConfig: cfg}
	bt.singleTransaction = true
	bt.HideConcurrentInsertBatchError = true
	bt.once.Do(func() {
		// noop ...
	})
	if err := bt.controlledInitialization(); nil != err {
		return nil, err
	}

	return bt, nil
}

// Batcher pushes row batches into DBMS.
type Batcher struct {
	BatcherConfig

	// Batches are dropped if rescueDir is empty.
	rescueDir string

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

	rescueBatchSuccess   *metric.Counter
	rescueBatchError     *metric.Counter
	rescuePushBatchBytes *metric.Counter
	rescueBatchSize      uint64

	pushRescueBatchSuccess     *metric.Counter
	pushRescueBatchDeleteError *metric.Counter
	pushRescueBatchError       *metric.Counter

	concurrentBatchesCh chan struct{}

	eventsBatchers    []*batcher.BytesBatcher
	eventsBatchersIdx uint32

	skipRescuedFiles map[string]bool

	once sync.Once

	conn                 *pgxpool.Pool
	statementDescription *pgconn.StatementDescription

	quotedTableName   string
	quotedColumnNames string
	totalColumns      int

	m *pgtype.Map

	singleTransaction          bool
	singleTransactionAnyError  atomic.Value
	singleTransactionErrors    uint32
	singleTransactionWaitGrout sync.WaitGroup
}

// PushRow pushes new row into batcher.
//
// appendRow must append the new row to b and return new b.
func (cb *Batcher) PushRow(appendRow func(b []byte) []byte) {
	cb.once.Do(cb.init)

	var eb *batcher.BytesBatcher
	if len(cb.eventsBatchers) == 1 {
		eb = cb.eventsBatchers[0]
	} else {
		n := atomic.AddUint32(&cb.eventsBatchersIdx, 1)
		idx := n % uint32(len(cb.eventsBatchers))
		eb = cb.eventsBatchers[idx]
	}

	if !eb.Push(func(dst []byte, rows int) []byte {
		dst = AppendInt16(dst, int16(cb.totalColumns))
		return appendRow(dst)
	}) {
		if !cb.singleTransaction {
			cb.pushRowOverflow.Inc()
		}

	} else {
		if !cb.singleTransaction {
			cb.pushRowSuccess.Inc()
		}
	}
}

// GetPushFailure returns the total number of rows failed to be sent
// to DBMS due to various reasons.
func (cb *Batcher) GetPushFailure() uint64 {
	cb.once.Do(cb.init)

	return cb.pushRowOverflow.Get() + cb.pushRowError.Get()
}

func (cb *Batcher) Append(b []byte, pos int, v any) ([]byte, error) {
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

func (cb *Batcher) GetConn() *pgxpool.Pool {
	cb.once.Do(cb.init)
	return cb.conn
}

func (cb *Batcher) Stop() (err error, errorsCount int) {

	for _, batcher := range cb.eventsBatchers {
		batcher.Stop()
	}

	cb.singleTransactionWaitGrout.Wait()
	av := cb.singleTransactionAnyError.Load()
	if nil == av {
		return nil, 0
	}
	return av.(error), int(atomic.LoadUint32(&cb.singleTransactionErrors))
}

func (cb *Batcher) controlledInitialization() error {
	if !cb.singleTransaction {
		cb.initMetrics()
	}

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

	if !cb.singleTransaction {
		cb.skipRescuedFiles = make(map[string]bool)
		go cb.rescuedDirPusher()
	}
	return nil
}

func (cb *Batcher) init() {
	err := cb.controlledInitialization()
	if nil != err {
		log.Fatalf("Error while initializaing Batcher: %v", err)
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
		<-c

		cb.conn.Close()
	}()
}

func (cb *Batcher) initMetrics() {
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

	cb.rescueBatchSuccess = metric.NewCounter(cb.metricPrefix + "RescueBatchSuccess")
	cb.rescueBatchError = metric.NewCounter(cb.metricPrefix + "RescueBatchError")
	cb.rescuePushBatchBytes = metric.NewCounter(cb.metricPrefix + "RescuePushBatchBytes")

	cb.pushRescueBatchSuccess = metric.NewCounter(cb.metricPrefix + "PushRescueBatchSuccess")
	cb.pushRescueBatchDeleteError = metric.NewCounter(cb.metricPrefix + "PushRescueBatchDeleteError")
	cb.pushRescueBatchError = metric.NewCounter(cb.metricPrefix + "PushRescueBatchError")

	metric.NewGauge(cb.metricPrefix+"RescueBatchSize", func() float64 {
		return float64(atomic.LoadUint64(&cb.rescueBatchSize))
	})

	go func() {
		for {
			time.Sleep(time.Minute)

			d, err := os.Open(cb.rescueDir)
			if err != nil {
				if !os.IsNotExist(err) {
					log.Printf("%s: cannot open the rescueDir=%q: %s", cb.metricPrefix, cb.rescueDir, err)
				}
				continue
			}

			names, err := d.Readdirnames(0)
			if err != nil {
				log.Printf("%s: cannot read files in rescueDir=%q: %s", cb.metricPrefix, cb.rescueDir, err)
				continue
			}

			atomic.StoreUint64(&cb.rescueBatchSize, uint64(len(names)))
		}
	}()

}

func (cb *Batcher) concurrentPushBatchToDB(sql []byte, itemsCount int) {
	attemptsCount := 0

	select {
	case cb.concurrentBatchesCh <- struct{}{}:
		sqlCopy := make([]byte, 0, len(sql))
		sqlCopy = append(sqlCopy, sql...)
		if cb.singleTransaction {
			cb.singleTransactionWaitGrout.Add(1)
		}
		go func() {
			cb.pushBatchToDB(sqlCopy, itemsCount)
			if cb.singleTransaction {
				cb.singleTransactionWaitGrout.Done()
			}
			<-cb.concurrentBatchesCh
		}()
	default:

		if cb.singleTransaction {
			attemptsCount++
			if attemptsCount >= cb.MaxRetries {
				atomic.AddUint32(&cb.singleTransactionErrors, 1)
				cb.singleTransactionAnyError.Store(fmt.Errorf("concurrent insert batches' limit %d exceeded", cap(cb.concurrentBatchesCh)))
			} else {
				//log.Printf("concurrent insert batches' limit %d exceeded, re-try in 1s", cap(cb.concurrentBatchesCh))
				time.Sleep(time.Second)
				cb.concurrentPushBatchToDB(sql, itemsCount)
			}
			return
		}

		cb.concurrentBatchesOverflow.Inc()
		if !cb.HideConcurrentInsertBatchError {
			log.Printf("%s: concurrent insert batches' limit %d exceeded",
				cb.metricPrefix, cap(cb.concurrentBatchesCh))
		}

		if len(cb.rescueDir) > 0 {
			cb.rescueBatchToFile(sql)
		}
	}
}

func (cb *Batcher) pushBatchToDB(sql []byte, itemsCount int) {
	attemptsCount := 0
	for {
		err := cb.batchInsert(sql)
		if err == nil {
			if !cb.singleTransaction {
				cb.pushBatchSuccess.Inc()
				cb.pushBatchSize.Update(float64(itemsCount))
			}
			return
		}

		if cb.singleTransaction {
			atomic.AddUint32(&cb.singleTransactionErrors, 1)
			cb.singleTransactionAnyError.Store(err)
			return
		}

		attemptsCount++
		if attemptsCount >= cb.MaxRetries {
			log.Printf("%s: cannot insert batch to DBMS: %s", cb.metricPrefix, err)
			cb.pushRowError.Add(itemsCount)
			cb.pushBatchError.Inc()
			if len(cb.rescueDir) > 0 {
				cb.rescueBatchToFile(sql)
			}
			return
		}
		time.Sleep(time.Second)

		cb.pushBatchRetries.Inc()
	}
}

func (cb *Batcher) batchInsert(sql []byte) error {
	if !cb.singleTransaction {
		cb.pushBatchBytes.Add(len(sql))
	}
	startTime := time.Now()

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
	if !cb.singleTransaction {
		cb.pushBatchDuration.UpdateDuration(startTime)
	}
	return err
}

func (cb *Batcher) rescueBatchToFile(sql []byte) {
	if err := os.MkdirAll(cb.rescueDir, 0755); err != nil {
		log.Panicf("%s: cannot create rescueDir=%q: %s", cb.metricPrefix, cb.rescueDir, err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02_15-04-05")
	filename := fmt.Sprintf("%s/%s_%d.tsv.gz", cb.rescueDir, timestamp, fastrand.Uint32())

	log.Printf("%s: rescuing batch with size %d to %q", cb.metricPrefix, len(sql), filename)

	// store the batch into a temporary file and then atomically move it
	// to the destination file in order to avoid race conditions
	// with the rescuedDirPusher.
	tmpFilename := filename + ".tmp"
	if err := ioutil.WriteFile(tmpFilename, sql, 0644); err != nil {
		log.Printf("%s: cannot dump batch to file %q: %s", cb.metricPrefix, tmpFilename, err)
		cb.rescueBatchError.Inc()
		return
	}
	if err := os.Rename(tmpFilename, filename); err != nil {
		log.Printf("%s: cannot rename %q to %q: %s", cb.metricPrefix, tmpFilename, filename, err)
		cb.rescueBatchError.Inc()
		return
	}

	cb.rescueBatchSuccess.Inc()
}

var rescueFilenameRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_\d+\.tsv\.gz$`)

func (cb *Batcher) rescuedDirPusher() {
	if len(cb.rescueDir) == 0 {
		return
	}

	for {
		cb.pushRescuedDir()
		time.Sleep(time.Minute)
	}
}

func (cb *Batcher) getMaxConcurrentRescuedBatches() int {
	if nil == cb.MaxConcurrentRescuedBatches {
		return 1
	}

	return *cb.MaxConcurrentRescuedBatches
}

func (cb *Batcher) pushRescuedDir() {
	maxConcurrentRescuedBatches := cb.getMaxConcurrentRescuedBatches()
	if -1 == maxConcurrentRescuedBatches {
		// means: do not push anything
		return
	}

	d, err := os.Open(cb.rescueDir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("%s: cannot open the rescueDir=%q: %s", cb.metricPrefix, cb.rescueDir, err)
		}
		return
	}

	names, err := d.Readdirnames(0)
	if err != nil {
		log.Printf("%s: cannot read files in rescueDir=%q: %s", cb.metricPrefix, cb.rescueDir, err)
		return
	}

	if maxConcurrentRescuedBatches <= 1 {
		for _, name := range names {
			if cb.getMaxConcurrentRescuedBatches() != maxConcurrentRescuedBatches {
				// MaxConcurrentRescuedBatches was changed, restart the process
				break
			}

			if !rescueFilenameRegexp.MatchString(name) {
				continue
			}

			filepath := fmt.Sprintf("%s/%s", cb.rescueDir, name)
			cb.pushRescuedFile(filepath)
		}
		return
	}

	worker := make(chan string, maxConcurrentRescuedBatches)
	for i := 0; i < maxConcurrentRescuedBatches; i++ {
		go func() {
			name := <-worker
			filepath := fmt.Sprintf("%s/%s", cb.rescueDir, name)
			cb.pushRescuedFile(filepath)
		}()
	}

	for _, name := range names {
		if !rescueFilenameRegexp.MatchString(name) {
			continue
		}

		if cb.getMaxConcurrentRescuedBatches() != maxConcurrentRescuedBatches {
			// MaxConcurrentRescuedBatches was changed, restart the process
			break
		}

		worker <- name
	}

}

func (cb *Batcher) pushRescuedFile(filepath string) {
	if cb.skipRescuedFiles[filepath] {
		return
	}

	sql, err := ioutil.ReadFile(filepath)
	if err != nil {
		log.Printf("%s: cannot read rescued file %q: %s", cb.metricPrefix, filepath, err)
		return
	}

	cb.rescuePushBatchBytes.Add(len(sql))
	if err := cb.batchInsert(sql); err != nil {
		cb.pushRescueBatchError.Inc()
		log.Printf("%s: cannot insert rescued file %q to DBMS: %s", cb.metricPrefix, filepath, err)
		return
	}

	if err := os.Remove(filepath); err != nil {
		cb.pushRescueBatchDeleteError.Inc()
		cb.skipRescuedFiles[filepath] = true
		log.Printf("%s: cannot delete rescued file %q after inserting it into DBMS: %s", cb.metricPrefix, filepath, err)
		return
	}

	cb.pushRescueBatchSuccess.Inc()

	log.Printf("%s: successfully pushed rescued file %q into DBMS", cb.metricPrefix, filepath)
}
