// Package publisher sends events to Kinesis Data Streams.
package publisher

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"producer/internal/event"
)

// MaxBatchSize is the PutRecords limit of records per request.
const MaxBatchSize = 500

var ErrBatchSize = fmt.Errorf("batch must contain between 1 and %d events", MaxBatchSize)

// KinesisAPI is the subset of *kinesis.Client the producer uses.
type KinesisAPI interface {
	PutRecord(ctx context.Context, in *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
	PutRecords(ctx context.Context, in *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
}

type Result struct {
	PackageID      string `json:"package_id"`
	ShardID        string `json:"shard_id,omitempty"`
	SequenceNumber string `json:"sequence_number,omitempty"`
	Error          string `json:"error,omitempty"`
}

type Publisher struct {
	API        KinesisAPI
	StreamName string
}

// Publish sends one event. The partition key is package_id, which decides the
// shard: all events of one package land on the same shard, in order.
func (p *Publisher) Publish(ctx context.Context, e event.Event) (Result, error) {
	data, err := e.Encode()
	if err != nil {
		return Result{}, err
	}
	out, err := p.API.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(p.StreamName),
		PartitionKey: aws.String(e.PackageID),
		Data:         data,
	})
	if err != nil {
		return Result{}, fmt.Errorf("put record: %w", err)
	}
	return Result{
		PackageID:      e.PackageID,
		ShardID:        aws.ToString(out.ShardId),
		SequenceNumber: aws.ToString(out.SequenceNumber),
	}, nil
}

// PublishBatch sends many events in one PutRecords call. The call can succeed
// (HTTP 200) while some records fail, so every record gets its own Result.
func (p *Publisher) PublishBatch(ctx context.Context, events []event.Event) ([]Result, error) {
	if len(events) == 0 || len(events) > MaxBatchSize {
		return nil, ErrBatchSize
	}
	entries := make([]types.PutRecordsRequestEntry, len(events))
	for i, e := range events {
		data, err := e.Encode()
		if err != nil {
			return nil, err
		}
		entries[i] = types.PutRecordsRequestEntry{Data: data, PartitionKey: aws.String(e.PackageID)}
	}
	out, err := p.API.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(p.StreamName),
		Records:    entries,
	})
	if err != nil {
		return nil, fmt.Errorf("put records: %w", err)
	}
	if len(out.Records) != len(events) {
		return nil, fmt.Errorf("kinesis returned %d results for %d events", len(out.Records), len(events))
	}
	results := make([]Result, len(events))
	for i, entry := range out.Records {
		result := Result{PackageID: events[i].PackageID}
		if entry.ErrorCode != nil {
			result.Error = aws.ToString(entry.ErrorCode) + ": " + aws.ToString(entry.ErrorMessage)
		} else {
			result.ShardID = aws.ToString(entry.ShardId)
			result.SequenceNumber = aws.ToString(entry.SequenceNumber)
		}
		results[i] = result
	}
	return results, nil
}
