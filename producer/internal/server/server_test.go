package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"producer/internal/event"
	"producer/internal/publisher"
)

type fakePublisher struct {
	publishCalls int
	batchCalls   int
	result       publisher.Result
	batchResults []publisher.Result
	err          error
}

func (f *fakePublisher) Publish(_ context.Context, _ event.Event) (publisher.Result, error) {
	f.publishCalls++
	return f.result, f.err
}

func (f *fakePublisher) PublishBatch(_ context.Context, _ []event.Event) ([]publisher.Result, error) {
	f.batchCalls++
	return f.batchResults, f.err
}

const goodEvent = `{"package_id":"PKG-001","timestamp":"2026-10-04T10:30:00Z","latitude":-12.0464,"longitude":-77.0428,"status":"CREATED"}`

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealth(t *testing.T) {
	rec := do(t, New(&fakePublisher{}), http.MethodGet, "/health", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
}

func TestPostEventReturnsShardAndSequenceNumber(t *testing.T) {
	pub := &fakePublisher{result: publisher.Result{PackageID: "PKG-001", ShardID: "shardId-000000000001", SequenceNumber: "4959"}}
	rec := do(t, New(pub), http.MethodPost, "/events", goodEvent)

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var got publisher.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.ShardID != "shardId-000000000001" || got.SequenceNumber != "4959" {
		t.Fatalf("unexpected body %s (err=%v)", rec.Body, err)
	}
}

func TestPostEventRejectsInvalidWithoutPublishing(t *testing.T) {
	pub := &fakePublisher{}
	h := New(pub)
	for _, body := range []string{
		`{"package_id":"PKG-001"}`,
		strings.Replace(goodEvent, "CREATED", "LOST", 1),
		strings.Replace(goodEvent, "10:30:00Z", "10:30:00+00:00", 1),
		`{"package_id":"PKG-001","speed":1}`,
		`not json`,
	} {
		if rec := do(t, h, http.MethodPost, "/events", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("body %q: code=%d", body, rec.Code)
		}
	}
	if pub.publishCalls != 0 {
		t.Fatal("invalid events must never reach Kinesis")
	}
}

func TestPostEventPublisherFailureIs502(t *testing.T) {
	rec := do(t, New(&fakePublisher{err: errors.New("kinesis down")}), http.MethodPost, "/events", goodEvent)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestPostEventOversizedBodyIs413(t *testing.T) {
	huge := `{"package_id":"` + strings.Repeat("a", 2<<20) + `"}`
	rec := do(t, New(&fakePublisher{}), http.MethodPost, "/events", huge)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestBatchAllSucceed(t *testing.T) {
	pub := &fakePublisher{batchResults: []publisher.Result{{PackageID: "PKG-001", ShardID: "s0", SequenceNumber: "1"}, {PackageID: "PKG-001", ShardID: "s0", SequenceNumber: "2"}}}
	rec := do(t, New(pub), http.MethodPost, "/events/batch", "["+goodEvent+","+goodEvent+"]")

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"failed":0`) || !strings.Contains(rec.Body.String(), `"count":2`) {
		t.Fatalf("body=%s", rec.Body)
	}
}

func TestBatchPartialFailureIs207(t *testing.T) {
	pub := &fakePublisher{batchResults: []publisher.Result{
		{PackageID: "PKG-001", ShardID: "s0", SequenceNumber: "1"},
		{PackageID: "PKG-001", Error: "ProvisionedThroughputExceededException: slow down"},
	}}
	rec := do(t, New(pub), http.MethodPost, "/events/batch", "["+goodEvent+","+goodEvent+"]")

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"failed":1`) {
		t.Fatalf("body=%s", rec.Body)
	}
}

func TestBatchRejectsBadInputWithoutPublishing(t *testing.T) {
	pub := &fakePublisher{}
	h := New(pub)

	tooMany := "[" + strings.TrimSuffix(strings.Repeat(goodEvent+",", publisher.MaxBatchSize+1), ",") + "]"
	cases := map[string]string{
		"empty array":      "[]",
		"not an array":     goodEvent,
		"one invalid":      "[" + goodEvent + "," + strings.Replace(goodEvent, "CREATED", "LOST", 1) + "]",
		"more than 500":    tooMany,
		"trailing garbage": "[" + goodEvent + "] x",
	}
	for name, body := range cases {
		if rec := do(t, h, http.MethodPost, "/events/batch", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: code=%d body=%s", name, rec.Code, rec.Body)
		}
	}
	if pub.batchCalls != 0 {
		t.Fatal("invalid batches must never reach Kinesis")
	}
}
