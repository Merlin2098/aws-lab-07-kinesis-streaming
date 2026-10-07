// Package simulator emits a continuous, realistic stream of package-tracking
// events so the pipeline can be watched without sending anything by hand.
package simulator

import (
	"context"
	"fmt"
	"math"
	"time"

	"producer/internal/event"
	"producer/internal/publisher"
)

// lifecycle is the sequence of statuses every simulated package goes through.
// CANCELLED is left out on purpose: the demo follows the happy path.
var lifecycle = []string{"CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"}

const timestampLayout = "2006-01-02T15:04:05Z"

// Publisher is what the simulator needs from internal/publisher.
type Publisher interface {
	PublishBatch(ctx context.Context, events []event.Event) ([]publisher.Result, error)
}

// Report describes one tick: the events built and the per-event publish results.
type Report struct {
	Events  []event.Event
	Results []publisher.Result
}

type trackedPackage struct {
	id   string
	next int // index in lifecycle of the status this package emits on the next tick
}

type Simulator struct {
	pub      Publisher
	now      func() time.Time
	runID    string
	seq      int
	packages []trackedPackage
}

// New creates a simulator that tracks the given number of packages at once.
func New(pub Publisher, packages int, now func() time.Time) *Simulator {
	s := &Simulator{pub: pub, now: now, runID: now().UTC().Format("150405")}
	s.packages = make([]trackedPackage, packages)
	for i := range s.packages {
		s.packages[i] = s.newPackage()
	}
	return s
}

func (s *Simulator) newPackage() trackedPackage {
	s.seq++
	return trackedPackage{id: fmt.Sprintf("SIM-%s-%d", s.runID, s.seq)}
}

func round4(v float64) float64 { return math.Round(v*1e4) / 1e4 }

func (s *Simulator) build(slot int, p trackedPackage, timestamp string) event.Event {
	// Deterministic drift: each slot sits on its own route and moves a little per status.
	lat := round4(-12.0464 - 0.01*float64(slot) - 0.004*float64(p.next))
	lon := round4(-77.0428 - 0.01*float64(slot) - 0.004*float64(p.next))
	return event.Event{
		PackageID: p.id,
		Timestamp: timestamp,
		Latitude:  &lat,
		Longitude: &lon,
		Status:    lifecycle[p.next],
	}
}

// Tick publishes one event per tracked package in a single batch. A package only
// advances to its next status when its event was published successfully, so a
// failed tick is retried with the same statuses on the next one. A package that
// reaches DELIVERED is replaced by a new one starting at CREATED.
func (s *Simulator) Tick(ctx context.Context) (Report, error) {
	timestamp := s.now().UTC().Format(timestampLayout)
	events := make([]event.Event, len(s.packages))
	for i, p := range s.packages {
		events[i] = s.build(i, p, timestamp)
	}

	results, err := s.pub.PublishBatch(ctx, events)
	if err != nil {
		return Report{}, fmt.Errorf("publish batch: %w", err)
	}
	if len(results) != len(events) {
		return Report{}, fmt.Errorf("got %d results for %d events", len(results), len(events))
	}

	for i := range s.packages {
		if results[i].Error != "" {
			continue
		}
		s.packages[i].next++
		if s.packages[i].next == len(lifecycle) {
			s.packages[i] = s.newPackage()
		}
	}
	return Report{Events: events, Results: results}, nil
}

// Run ticks immediately and then every interval until ctx is cancelled. A failed
// tick is logged and does not stop the loop.
func (s *Simulator) Run(ctx context.Context, interval time.Duration, logf func(format string, args ...any)) {
	s.emit(ctx, logf)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.emit(ctx, logf)
		}
	}
}

func (s *Simulator) emit(ctx context.Context, logf func(format string, args ...any)) {
	report, err := s.Tick(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logf("simulator: tick failed, retrying next interval: %v", err)
		}
		return
	}
	failed := 0
	for i, e := range report.Events {
		result := report.Results[i]
		if result.Error != "" {
			failed++
			logf("simulator: %s %s FAILED: %s", e.PackageID, e.Status, result.Error)
			continue
		}
		logf("simulator: %s %s -> %s", e.PackageID, e.Status, result.ShardID)
	}
	logf("simulator: emitted %d events (%d failed)", len(report.Events), failed)
}
