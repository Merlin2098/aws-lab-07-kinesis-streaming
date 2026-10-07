package simulator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"producer/internal/event"
	"producer/internal/publisher"
)

// fakePub records every batch it receives and can fail whole calls or single items.
type fakePub struct {
	mu        sync.Mutex
	calls     [][]event.Event
	errOn     map[int]error // 1-based call number -> error for the whole call
	itemErrOn map[int][]int // 1-based call number -> indexes whose result carries an error
}

func (f *fakePub) PublishBatch(_ context.Context, events []event.Event) ([]publisher.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]event.Event(nil), events...))
	n := len(f.calls)
	if err := f.errOn[n]; err != nil {
		return nil, err
	}
	results := make([]publisher.Result, len(events))
	for i, e := range events {
		results[i] = publisher.Result{PackageID: e.PackageID, ShardID: "shardId-000000000000", SequenceNumber: "1"}
	}
	for _, i := range f.itemErrOn[n] {
		results[i].ShardID, results[i].SequenceNumber = "", ""
		results[i].Error = "ProvisionedThroughputExceededException: slow down"
	}
	return results, nil
}

func (f *fakePub) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// clock returns a fake time source that advances 25 seconds per reading.
func clock() func() time.Time {
	t := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	return func() time.Time {
		current := t
		t = t.Add(25 * time.Second)
		return current
	}
}

func statuses(events []event.Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Status
	}
	return out
}

func TestFirstTickEmitsOneValidCreatedEventPerPackage(t *testing.T) {
	pub := &fakePub{}
	s := New(pub, 3, clock())

	report, err := s.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Events) != 3 || len(report.Results) != 3 {
		t.Fatalf("got %d events, %d results", len(report.Events), len(report.Results))
	}
	seen := map[string]bool{}
	for _, e := range report.Events {
		if err := e.Validate(); err != nil {
			t.Fatalf("event does not satisfy the contract: %v (%+v)", err, e)
		}
		if e.Status != "CREATED" || !strings.HasPrefix(e.PackageID, "SIM-") {
			t.Fatalf("unexpected first event: %+v", e)
		}
		seen[e.PackageID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("package ids must be distinct, got %v", seen)
	}
}

func TestEachPackageAdvancesOneStatusPerTickAndTimestampsGrow(t *testing.T) {
	pub := &fakePub{}
	s := New(pub, 2, clock())
	want := []string{"CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"}

	lastTimestamp := ""
	for tick, status := range want {
		report, err := s.Tick(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range report.Events {
			if e.Status != status {
				t.Fatalf("tick %d: status %s, want %s", tick+1, e.Status, status)
			}
			if err := e.Validate(); err != nil {
				t.Fatalf("tick %d: %v", tick+1, err)
			}
		}
		if report.Events[0].Timestamp <= lastTimestamp {
			t.Fatalf("tick %d: timestamp %s did not grow past %s", tick+1, report.Events[0].Timestamp, lastTimestamp)
		}
		lastTimestamp = report.Events[0].Timestamp
	}
}

func TestDeliveredPackagesAreReplacedByNewOnes(t *testing.T) {
	pub := &fakePub{}
	s := New(pub, 2, clock())
	var firstIDs []string
	for tick := 0; tick < 5; tick++ {
		report, err := s.Tick(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if tick == 0 {
			for _, e := range report.Events {
				firstIDs = append(firstIDs, e.PackageID)
			}
		}
	}

	report, err := s.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range report.Events {
		if e.Status != "CREATED" {
			t.Fatalf("replacement package must start CREATED, got %s", e.Status)
		}
		if e.PackageID == firstIDs[i] {
			t.Fatalf("package %s was not replaced after DELIVERED", e.PackageID)
		}
	}
}

func TestFailedPublishDoesNotAdvanceAnyPackage(t *testing.T) {
	pub := &fakePub{errOn: map[int]error{1: errors.New("kinesis down")}}
	s := New(pub, 2, clock())

	if _, err := s.Tick(context.Background()); err == nil {
		t.Fatal("expected the publish error to be returned")
	}
	report, err := s.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range report.Events {
		if e.Status != "CREATED" {
			t.Fatalf("after a failed tick every package must retry CREATED, got %s", e.Status)
		}
	}
}

func TestPartialFailureAdvancesOnlyThePackagesThatSucceeded(t *testing.T) {
	pub := &fakePub{itemErrOn: map[int][]int{1: {1}}}
	s := New(pub, 3, clock())

	if _, err := s.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	report, err := s.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := statuses(report.Events)
	want := []string{"PICKED_UP", "CREATED", "PICKED_UP"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("statuses = %v, want %v", got, want)
		}
	}
}

func TestEventsStayValidForLargeFleetsAndManyCycles(t *testing.T) {
	pub := &fakePub{}
	s := New(pub, 100, clock())
	for tick := 0; tick < 30; tick++ {
		report, err := s.Tick(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range report.Events {
			if err := e.Validate(); err != nil {
				t.Fatalf("tick %d: %v (%+v)", tick+1, err, e)
			}
		}
	}
}

func TestRunKeepsGoingAfterAFailureAndStopsOnCancel(t *testing.T) {
	pub := &fakePub{errOn: map[int]error{1: errors.New("kinesis down")}}
	s := New(pub, 1, clock())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx, 5*time.Millisecond, func(string, ...any) {})
		close(done)
	}()

	deadline := time.After(3 * time.Second)
	for pub.callCount() < 3 {
		select {
		case <-deadline:
			t.Fatalf("Run stopped after %d calls; it must continue past a failed tick", pub.callCount())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

func TestParseConfig(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr string
	}{
		{"disabled when unset", map[string]string{}, Config{Packages: 3}, ""},
		{"disabled with zero", map[string]string{"EMIT_INTERVAL": "0"}, Config{Packages: 3}, ""},
		{"25 seconds", map[string]string{"EMIT_INTERVAL": "25s"}, Config{Enabled: true, Interval: 25 * time.Second, Packages: 3}, ""},
		{"custom packages", map[string]string{"EMIT_INTERVAL": "10s", "EMIT_PACKAGES": "5"}, Config{Enabled: true, Interval: 10 * time.Second, Packages: 5}, ""},
		{"not a duration", map[string]string{"EMIT_INTERVAL": "soon"}, Config{}, "EMIT_INTERVAL"},
		{"negative", map[string]string{"EMIT_INTERVAL": "-5s"}, Config{}, "EMIT_INTERVAL"},
		{"too fast", map[string]string{"EMIT_INTERVAL": "100ms"}, Config{}, "at least 1s"},
		{"packages not a number", map[string]string{"EMIT_INTERVAL": "25s", "EMIT_PACKAGES": "many"}, Config{}, "EMIT_PACKAGES"},
		{"packages zero", map[string]string{"EMIT_INTERVAL": "25s", "EMIT_PACKAGES": "0"}, Config{}, "EMIT_PACKAGES"},
		{"packages over the cap", map[string]string{"EMIT_INTERVAL": "25s", "EMIT_PACKAGES": "101"}, Config{}, "EMIT_PACKAGES"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseConfig(func(key string) string { return tc.env[key] })
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want it to mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("config = %+v, want %+v", got, tc.want)
			}
		})
	}
}
