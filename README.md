### PostgreSQL Batcher based on COPY BIN protocol

Groups rows into batches and calls a user-specified function to write the batches to PostgreSQL.

https://www.postgresql.org/docs/current/sql-copy.html

Writing data via COPY rather than INSERT provides a number of advantages
1) It is orders of magnitude faster than INSERT, since it uses a binary data format https://www.postgresql.org/docs/current/populate.html#POPULATE-COPY-FROM
2) SQL injections are not possible, so writing data does not require escaping

## Batcher with controlled recording
If you need to record a large amount of data and control that the recording was successful, please, use NewBatcherSingle method

```go
    b, err := NewBatcherSingle(BatcherConfig{
		TableName:    "segments_100500",
		TableColumns: "segment_id,client_id,uid",
		ConnAddr:     connString,

		MaxBatchSize:  100e3,
		MaxBatchDelay: time.Second * 5,
		MaxRetries:    10,
	})

	
	records := int(1e6)
	for i := 0; i < records; i++ {
		b.PushRow(func(b []byte) []byte {
			b, _ = b.Append(b, 0, 100500) // colum 1
			b, _ = b.Append(b, 1, 200500) // colum 2
			b, _ = b.Append(b, 2, uuid.New()) // colum 3
			return b
		})
	}

	// The stop method will split the write into batches and ensure that all background writes have completed.
	// If there was an error, the last one that occurred is returned.
	err, errorsCount := b.Stop()
```

## Batcher without controlled recording
In case you are using PostgreSQL for logging data or you do not need to control the recording, please use NewBatcherClassic method
In case of errors, the recording can be repeated in the background process again


```go
    // Folder for storing batches in case of errors. Batches are dropped if rescueDir is empty.
    rescueDir := "/tmp"
    b := NewBatcherClassic(BatcherConfig{
		TableName:    "segments_100500",
		TableColumns: "segment_id,client_id,uid",
		ConnAddr:     connString,

		MaxBatchSize:  100e3,
		MaxBatchDelay: time.Second * 5,
		MaxRetries:    10,
	}, rescueDir, "testPrefix")

	
	records := int(1e6)
	for i := 0; i < records; i++ {
		// Recording occurs in the background process upon the occurrence of one of the events MaxBatchSize or MaxBatchDelay
		b.PushRow(func(b []byte) []byte {
			b, _ = b.Append(b, 0, 100500) // colum 1
			b, _ = b.Append(b, 1, 200500) // colum 2
			b, _ = b.Append(b, 2, uuid.New()) // colum 3
			return b
		})
	}

```
