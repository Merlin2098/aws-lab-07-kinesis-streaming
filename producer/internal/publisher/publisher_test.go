package publisher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"producer/internal/event"
)

type fakeAPI struct {
	putRecordIn   *kinesis.PutRecordInput
	putRecordsIn  *kinesis.PutRecordsInput
	putRecordOut  *kinesis.PutRecordOutput
	putRecordsOut *kinesis.PutRecordsOutput
	err           error
}

func (f *fakeAPI) PutRecord(_ context.Context, in *kinesis.PutRecordInput, _ ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error) {
	f.putRecordIn = in
	return f.putRecordOut, f.err
}

func (f *fakeAPI) PutRecords(_ context.Context, in *kinesis.PutRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	f.putRecordsIn = in
	return f.putRecordsOut, f.err
}

func sample(id string) event.Event {
	lat, lon := -12.0464, -77.0428
	return event.Event{PackageID: id, Timestamp: "2026-10-04T10:30:00Z", Latitude: &lat, Longitude: &lon, Status: "IN_TRANSIT"}
}

func TestPublishUsesPackageIDAsPartitionKey(t *testing.T) {
	api := &fakeAPI{putRecordOut: &kinesis.PutRecordOutput{ShardId: aws.String("shardId-000000000001"), SequenceNumber: aws.String("4959")}}
	p := &Publisher{API: api, StreamName: "logistics-events"}

	res, err := p.Publish(context.Background(), sample("PKG-001"))
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(api.putRecordIn.StreamName) != "logistics-events" {
		t.Fatalf("stream = %q", aws.ToString(api.putRecordIn.StreamName))
	}
	if aws.ToString(api.putRecordIn.PartitionKey) != "PKG-001" {
		t.Fatalf("partition key = %q", aws.ToString(api.putRecordIn.PartitionKey))
	}
	if !strings.HasSuffix(string(api.putRecordIn.Data), "\n") {
		t.Fatalf("record must end with a newline: %q", api.putRecordIn.Data)
	}
	if res.ShardID != "shardId-000000000001" || res.SequenceNumber != "4959" || res.PackageID != "PKG-001" {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestPublishWrapsAPIError(t *testing.T) {
	boom := errors.New("boom")
	p := &Publisher{API: &fakeAPI{err: boom}, StreamName: "s"}
	if _, err := p.Publish(context.Background(), sample("PKG-001")); !errors.Is(err, boom) {
		t.Fatalf("expected wrapped boom, got %v", err)
	}
}

func TestPublishBatchReportsPartialFailure(t *testing.T) {
	api := &fakeAPI{putRecordsOut: &kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int32(1),
		Records: []types.PutRecordsResultEntry{
			{ShardId: aws.String("shardId-000000000000"), SequenceNumber: aws.String("1")},
			{ErrorCode: aws.String("ProvisionedThroughputExceededException"), ErrorMessage: aws.String("slow down")},
		},
	}}
	p := &Publisher{API: api, StreamName: "s"}

	results, err := p.PublishBatch(context.Background(), []event.Event{sample("PKG-001"), sample("PKG-002")})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Error != "" || results[0].ShardID != "shardId-000000000000" {
		t.Fatalf("first result: %+v", results[0])
	}
	if !strings.Contains(results[1].Error, "ProvisionedThroughputExceededException") || results[1].PackageID != "PKG-002" {
		t.Fatalf("second result: %+v", results[1])
	}
	if aws.ToString(api.putRecordsIn.Records[1].PartitionKey) != "PKG-002" {
		t.Fatal("each record must use its own package_id as partition key")
	}
}

func TestPublishBatchRejectsBadSizesWithoutCallingKinesis(t *testing.T) {
	api := &fakeAPI{}
	p := &Publisher{API: api, StreamName: "s"}

	if _, err := p.PublishBatch(context.Background(), nil); !errors.Is(err, ErrBatchSize) {
		t.Fatalf("empty batch: %v", err)
	}
	tooMany := make([]event.Event, MaxBatchSize+1)
	for i := range tooMany {
		tooMany[i] = sample("PKG-001")
	}
	if _, err := p.PublishBatch(context.Background(), tooMany); !errors.Is(err, ErrBatchSize) {
		t.Fatalf("oversized batch: %v", err)
	}
	if api.putRecordsIn != nil {
		t.Fatal("Kinesis must not be called for an invalid batch size")
	}
}

func TestPublishBatchDetectsResultCountMismatch(t *testing.T) {
	api := &fakeAPI{putRecordsOut: &kinesis.PutRecordsOutput{Records: []types.PutRecordsResultEntry{}}}
	p := &Publisher{API: api, StreamName: "s"}
	if _, err := p.PublishBatch(context.Background(), []event.Event{sample("PKG-001")}); err == nil {
		t.Fatal("expected an error when Kinesis returns fewer results than events")
	}
}
