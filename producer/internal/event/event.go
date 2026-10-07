// Package event defines the package-tracking event and its validation rules.
// The rules mirror src/contracts/package_event_contract.json;
// tests/test_contract_parity.py keeps both in sync.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	PackageIDPattern = `^[A-Za-z0-9-]{1,64}$`
	TimestampPattern = `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`
)

var Statuses = []string{"CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "CANCELLED"}

var (
	packageIDRe = regexp.MustCompile(PackageIDPattern)
	timestampRe = regexp.MustCompile(TimestampPattern)
)

// Event is one tracking update. Latitude and Longitude are pointers so a
// missing or null value can be told apart from a legitimate 0.
type Event struct {
	PackageID string   `json:"package_id"`
	Timestamp string   `json:"timestamp"`
	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
	Status    string   `json:"status"`
	City      string   `json:"city,omitempty"`
	DriverID  string   `json:"driver_id,omitempty"`
}

func (e Event) Validate() error {
	if !packageIDRe.MatchString(e.PackageID) {
		return fmt.Errorf("package_id must match %s", PackageIDPattern)
	}
	if !timestampRe.MatchString(e.Timestamp) {
		return errors.New("timestamp must be UTC formatted as YYYY-MM-DDTHH:MM:SSZ")
	}
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return fmt.Errorf("timestamp is not a real date: %s", e.Timestamp)
	}
	if err := checkRange("latitude", e.Latitude, -90, 90); err != nil {
		return err
	}
	if err := checkRange("longitude", e.Longitude, -180, 180); err != nil {
		return err
	}
	if !slices.Contains(Statuses, e.Status) {
		return fmt.Errorf("status must be one of %s", strings.Join(Statuses, ", "))
	}
	if len(e.City) > 100 {
		return errors.New("city is longer than 100 characters")
	}
	if len(e.DriverID) > 64 {
		return errors.New("driver_id is longer than 64 characters")
	}
	return nil
}

func checkRange(name string, value *float64, lo, hi float64) error {
	if value == nil {
		return fmt.Errorf("%s is required", name)
	}
	if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < lo || *value > hi {
		return fmt.Errorf("%s must be between %g and %g", name, lo, hi)
	}
	return nil
}

// Encode returns the JSON record with a trailing newline. Firehose concatenates
// records without a separator, so the newline keeps the S3 objects JSON Lines.
func (e Event) Encode() ([]byte, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func Parse(r io.Reader) (Event, error) {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	var e Event
	if err := decoder.Decode(&e); err != nil {
		return Event{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Event{}, errors.New("unexpected data after the JSON body")
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

func ParseBatch(r io.Reader) ([]Event, error) {
	decoder := json.NewDecoder(r)
	var raw []json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("body must be a JSON array of events: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the JSON body")
	}
	events := make([]Event, 0, len(raw))
	for i, item := range raw {
		e, err := Parse(bytes.NewReader(item))
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		events = append(events, e)
	}
	return events, nil
}
