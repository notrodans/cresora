package delivery

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/metric/noop"

	applicationdelivery "github.com/notrodans/nebula-go/internal/application/commands/delivery"
	"github.com/notrodans/nebula-go/internal/domain/message"
	"github.com/notrodans/nebula-go/internal/domain/recipient"
)

func TestNewClaimsWithNilMeterIsNoOp(t *testing.T) {
	wantTask := taskStub{}
	wantError := errors.New("claim failed")
	delegate := &claimsStub{task: wantTask, err: wantError}

	decorated := NewClaims(delegate, nil)
	if decorated != delegate {
		t.Fatalf("NewClaims returned %T, want the original delegate", decorated)
	}

	gotTask, gotError := decorated.Claim(context.Background())
	if gotTask != wantTask {
		t.Fatalf("Claim returned task %v, want %v", gotTask, wantTask)
	}
	if gotError != wantError {
		t.Fatalf("Claim returned error %v, want exact error %v", gotError, wantError)
	}
}

func TestClaimsRecordsDurationAndOutcome(t *testing.T) {
	wantTask := taskStub{}
	wantError := errors.New("database unavailable")
	tests := []struct {
		name    string
		err     error
		outcome string
	}{
		{name: "claimed", outcome: claimOutcomeClaimed},
		{name: "empty", err: fmt.Errorf("claim: %w", applicationdelivery.ErrEmpty), outcome: claimOutcomeEmpty},
		{name: "error", err: wantError, outcome: claimOutcomeError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			meter := newRecordingMeter()
			delegate := &claimsStub{task: wantTask, err: test.err}
			decorated := NewClaims(delegate, meter)

			gotTask, gotError := decorated.Claim(context.Background())
			if gotTask != wantTask {
				t.Fatalf("Claim returned task %v, want %v", gotTask, wantTask)
			}
			if gotError != test.err {
				t.Fatalf("Claim returned error %v, want exact error %v", gotError, test.err)
			}

			histogram := meter.histogram(ClaimDurationMetricName)
			if len(histogram.records) != 1 {
				t.Fatalf("recorded %d claim measurements, want 1", len(histogram.records))
			}
			assertMeasurement(t, histogram.records[0], test.outcome)
		})
	}
}

func TestNewPortWithNilMeterIsNoOp(t *testing.T) {
	wantError := errors.New("send failed")
	delegate := &portStub{err: wantError}

	decorated := NewPort(delegate, nil)
	if decorated != delegate {
		t.Fatalf("NewPort returned %T, want the original delegate", decorated)
	}

	gotError := decorated.Send(context.Background(), nil, nil, 7)
	if gotError != wantError {
		t.Fatalf("Send returned error %v, want exact error %v", gotError, wantError)
	}
	if delegate.count != 1 {
		t.Fatalf("Send called delegate %d times, want 1", delegate.count)
	}
}

func TestPortRecordsDurationAndOutcome(t *testing.T) {
	wantError := errors.New("transport failed")
	tests := []struct {
		name    string
		err     error
		outcome string
	}{
		{name: "success", outcome: sendOutcomeSuccess},
		{name: "error", err: wantError, outcome: sendOutcomeError},
		{
			name:    "canceled",
			err:     fmt.Errorf("send: %w", context.Canceled),
			outcome: sendOutcomeCanceled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			meter := newRecordingMeter()
			delegate := &portStub{err: test.err}
			decorated := NewPort(delegate, meter)

			gotError := decorated.Send(context.Background(), nil, nil, 7)
			if gotError != test.err {
				t.Fatalf("Send returned error %v, want exact error %v", gotError, test.err)
			}
			if delegate.count != 1 {
				t.Fatalf("Send called delegate %d times, want 1", delegate.count)
			}

			histogram := meter.histogram(SendDurationMetricName)
			if len(histogram.records) != 1 {
				t.Fatalf("recorded %d send measurements, want 1", len(histogram.records))
			}
			assertMeasurement(t, histogram.records[0], test.outcome)
		})
	}
}

func assertMeasurement(t *testing.T, measurement measurement, wantOutcome string) {
	t.Helper()
	if measurement.value < 0 {
		t.Fatalf("recorded duration %v, want non-negative duration", measurement.value)
	}

	attributes := measurement.attributes.ToSlice()
	if len(attributes) != 1 {
		t.Fatalf("recorded attributes %v, want exactly one outcome attribute", attributes)
	}
	if attributes[0].Key != attribute.Key(outcomeAttributeKey) {
		t.Fatalf("recorded attribute key %q, want %q", attributes[0].Key, outcomeAttributeKey)
	}
	if got := attributes[0].Value.AsString(); got != wantOutcome {
		t.Fatalf("recorded outcome %q, want %q", got, wantOutcome)
	}
}

type claimsStub struct {
	task applicationdelivery.Task
	err  error
}

func (stub *claimsStub) Claim(context.Context) (applicationdelivery.Task, error) {
	return stub.task, stub.err
}

type taskStub struct{}

func (taskStub) Route() applicationdelivery.Route {
	return applicationdelivery.Route{}
}

func (taskStub) Execute(context.Context, applicationdelivery.Command) error {
	return nil
}

func (taskStub) Release(context.Context, error) error {
	return nil
}

type portStub struct {
	err   error
	count int
}

func (stub *portStub) Send(context.Context, recipient.Recipient, message.Message, int64) error {
	stub.count++
	return stub.err
}

type measurement struct {
	value      float64
	attributes attribute.Set
}

type recordingHistogram struct {
	embedded.Float64Histogram
	records []measurement
}

func (histogram *recordingHistogram) Record(_ context.Context, value float64, options ...metric.RecordOption) {
	histogram.records = append(histogram.records, measurement{
		value:      value,
		attributes: metric.NewRecordConfig(options).Attributes(),
	})
}

func (*recordingHistogram) Enabled(context.Context) bool {
	return true
}

type recordingMeter struct {
	metric.Meter
	histograms map[string]*recordingHistogram
}

func newRecordingMeter() *recordingMeter {
	return &recordingMeter{
		Meter:      noop.NewMeterProvider().Meter("test"),
		histograms: make(map[string]*recordingHistogram),
	}
}

func (meter *recordingMeter) Float64Histogram(name string, _ ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	histogram := &recordingHistogram{}
	meter.histograms[name] = histogram
	return histogram, nil
}

func (meter *recordingMeter) histogram(name string) *recordingHistogram {
	histogram := meter.histograms[name]
	if histogram == nil {
		panic(fmt.Sprintf("histogram %q was not created", name))
	}
	return histogram
}
