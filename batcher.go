package pgcopy

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

const defaultWriteTimeout = time.Minute

type BatcherConfig struct {
	// DBMS PostgreSQL Connection string
	ConnAddr string

	// Write timeout for COPY command
	WriteTimeout time.Duration

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

type Batcher interface {
	PushRow(appendRow func(b []byte) []byte)
	GetPushFailure() uint64
	Append(b []byte, pos int, v any) ([]byte, error)
	GetConn() *pgxpool.Pool
	Stop() (err error, errorsCount int)
}
