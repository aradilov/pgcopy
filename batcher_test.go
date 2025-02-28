package pgcopy

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const connString = "postgres://dcrdbuser:wGKB85Eki3R1Gy83h@localhost:5436/dcrdb"

func TestSingleBatcher(t *testing.T) {
	batcher, err := NewBatcherSingle(BatcherConfig{
		TableName:     "test",
		TableColumns:  "segment_id,client_id,uid",
		ConnAddr:      connString,
		MaxBatchSize:  100e3,
		MaxBatchDelay: time.Millisecond * 50,
	})
	assert.NoError(t, err)

	_, err = batcher.GetConn().Exec(context.Background(), "truncate table test;")
	assert.NoError(t, err)

	records := int(1e6)
	for i := 0; i < records; i++ {
		batcher.PushRow(func(b []byte) []byte {
			b, err = batcher.Append(b, 0, 1)
			assert.NoError(t, err)
			b, err = batcher.Append(b, 1, 2)
			assert.NoError(t, err)
			b, err = batcher.Append(b, 2, uuid.New())
			assert.NoError(t, err)
			return b
		})
	}

	err, errorsCount := batcher.Stop()
	assert.NoError(t, err)
	assert.Equal(t, 0, errorsCount, fmt.Sprintf("Expected 0 errors, got %d", errorsCount))

	var count int64
	pool, err := pgxpool.New(context.Background(), connString)
	assert.NoError(t, err)

	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM test").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, int64(records), count)
}

func TestBatcher(t *testing.T) {
	delay := time.Second * 5

	batcher := NewBatcherClassic(BatcherConfig{
		TableName:     "test",
		TableColumns:  "segment_id,client_id,uid",
		ConnAddr:      connString,
		MaxBatchSize:  100e3,
		MaxBatchDelay: delay,
	}, "", "testPrefix", 10)

	_, err := batcher.GetConn().Exec(context.Background(), "truncate table test")
	assert.NoError(t, err)

	records := int(1e6)
	for i := 0; i < records; i++ {
		batcher.PushRow(func(b []byte) []byte {
			b, _ = batcher.Append(b, 0, 1)
			b, _ = batcher.Append(b, 1, 2)
			b, _ = batcher.Append(b, 2, uuid.New())
			return b
		})
	}

	time.Sleep(delay * 2)

	var count int64
	err = batcher.GetConn().QueryRow(context.Background(), "SELECT COUNT(*) FROM test").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, int64(records), count)

}
