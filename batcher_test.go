package pgcopy

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const connString = "postgres://USER:PASSWORD@localhost:5432/test"

func TestSingleBatcher(t *testing.T) {
	batcher, err := NewBatcherSingle(BatcherConfig{
		TableName:    "segments_100500",
		TableColumns: "segment_id,client_id,uid",
		ConnAddr:     connString,

		MaxBatchSize:  100e3,
		MaxBatchDelay: time.Second * 5,
		MaxRetries:    10,
	})
	assert.NoError(t, err)

	_, err = batcher.GetConn().Exec(context.Background(), "DELETE FROM segments_100500")
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

	err, errorsCount := batcher.stop()
	assert.NoError(t, err)
	assert.Equal(t, 0, errorsCount, fmt.Sprintf("Expected 0 errors, got %d", errorsCount))

	var count int64
	err = batcher.GetConn().QueryRow(context.Background(), "SELECT COUNT(*) FROM segments_100500").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, int64(records), count)

}

func TestBatcher(t *testing.T) {
	batcher := NewBatcher(BatcherConfig{
		TableName:    "segments_100500",
		TableColumns: "segment_id,client_id,uid",
		ConnAddr:     connString,

		MaxBatchSize:  100e3,
		MaxBatchDelay: time.Second * 5,
		MaxRetries:    10,
	}, "", "testPrefix")

	_, err := batcher.GetConn().Exec(context.Background(), "DELETE FROM segments_100500")
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

	time.Sleep(batcher.MaxBatchDelay * 2)

	var count int64
	err = batcher.GetConn().QueryRow(context.Background(), "SELECT COUNT(*) FROM segments_100500").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, int64(records), count)

}
