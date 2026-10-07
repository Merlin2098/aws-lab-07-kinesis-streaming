package event

import (
	"encoding/json"
	"strings"
	"testing"
)

func body(t *testing.T, mutate func(m map[string]any)) string {
	t.Helper()
	m := map[string]any{
		"package_id": "PKG-001",
		"timestamp":  "2026-10-04T10:30:00Z",
		"latitude":   -12.0464,
		"longitude":  -77.0428,
		"status":     "IN_TRANSIT",
	}
	if mutate != nil {
		mutate(m)
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestParseValid(t *testing.T) {
	e, err := Parse(strings.NewReader(body(t, func(m map[string]any) {
		m["city"] = "Lima"
		m["driver_id"] = "DRV-001"
	})))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.PackageID != "PKG-001" || e.Status != "IN_TRANSIT" || e.City != "Lima" {
		t.Fatalf("unexpected event: %+v", e)
	}
	if *e.Latitude != -12.0464 {
		t.Fatalf("latitude = %v", *e.Latitude)
	}
}

func TestParseRejects(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		wanted string
	}{
		{"missing latitude", body(t, func(m map[string]any) { delete(m, "latitude") }), "latitude is required"},
		{"null latitude", body(t, func(m map[string]any) { m["latitude"] = nil }), "latitude is required"},
		{"offset timestamp", body(t, func(m map[string]any) { m["timestamp"] = "2026-10-04T10:30:00+00:00" }), "timestamp must be UTC"},
		{"fractional timestamp", body(t, func(m map[string]any) { m["timestamp"] = "2026-10-04T10:30:00.123Z" }), "timestamp must be UTC"},
		{"impossible date", body(t, func(m map[string]any) { m["timestamp"] = "2026-13-45T10:30:00Z" }), "not a real date"},
		{"unknown status", body(t, func(m map[string]any) { m["status"] = "LOST" }), "status must be one of"},
		{"latitude out of range", body(t, func(m map[string]any) { m["latitude"] = 91 }), "latitude must be between"},
		{"longitude out of range", body(t, func(m map[string]any) { m["longitude"] = -180.5 }), "longitude must be between"},
		{"empty package id", body(t, func(m map[string]any) { m["package_id"] = "" }), "package_id must match"},
		{"unknown field", body(t, func(m map[string]any) { m["speed"] = 10 }), "invalid JSON"},
		{"not json", "nope", "invalid JSON"},
		{"trailing data", body(t, nil) + " {}", "unexpected data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(strings.NewReader(tc.body))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wanted) {
				t.Fatalf("error %q does not contain %q", err, tc.wanted)
			}
		})
	}
}

func TestEncodeEndsWithNewlineAndRoundTrips(t *testing.T) {
	e, err := Parse(strings.NewReader(body(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	data, err := e.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(data), "\n") || strings.Count(string(data), "\n") != 1 {
		t.Fatalf("expected exactly one trailing newline, got %q", data)
	}
	again, err := Parse(strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("encoded event does not parse: %v", err)
	}
	if again.PackageID != e.PackageID || *again.Latitude != *e.Latitude {
		t.Fatalf("round trip changed the event: %+v vs %+v", again, e)
	}
}

func TestParseBatch(t *testing.T) {
	good := body(t, nil)
	events, err := ParseBatch(strings.NewReader("[" + good + "," + good + "]"))
	if err != nil || len(events) != 2 {
		t.Fatalf("got %d events, err=%v", len(events), err)
	}

	bad := body(t, func(m map[string]any) { m["status"] = "LOST" })
	_, err = ParseBatch(strings.NewReader("[" + good + "," + bad + "]"))
	if err == nil || !strings.Contains(err.Error(), "event 1") {
		t.Fatalf("expected an error naming event 1, got %v", err)
	}

	if _, err = ParseBatch(strings.NewReader(good)); err == nil {
		t.Fatal("a single object is not a batch")
	}
}
