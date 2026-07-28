// Package delivery provides telemetry decorators for delivery ports.
package delivery

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	applicationdelivery "github.com/notrodans/nebula-go/internal/application/commands/delivery"
	"github.com/notrodans/nebula-go/internal/domain/message"
	"github.com/notrodans/nebula-go/internal/domain/recipient"
)

const (
	// ClaimDurationMetricName is the histogram name for Claims.Claim latency.
	ClaimDurationMetricName = "delivery.claim.duration"
	// SendDurationMetricName is the histogram name for Port.Send latency.
	SendDurationMetricName = "delivery.send.duration"

	outcomeAttributeKey = "outcome"

	claimOutcomeClaimed = "claimed"
	claimOutcomeEmpty   = "empty"
	claimOutcomeError   = "error"

	sendOutcomeSuccess  = "success"
	sendOutcomeError    = "error"
	sendOutcomeCanceled = "canceled"
)

// NewClaims decorates delegate with claim duration measurements. A nil meter
// leaves delegate unchanged. Instrument setup failures are also treated as a
// disabled measurement rather than affecting claim behavior.
func NewClaims(delegate applicationdelivery.Claims, meter metric.Meter) applicationdelivery.Claims {
	if delegate == nil || meter == nil {
		return delegate
	}

	return claims{
		delegate: delegate,
		duration: newHistogram(
			meter,
			ClaimDurationMetricName,
			"Time spent claiming a delivery task",
		),
	}
}

// NewPort decorates delegate with Send duration measurements. A nil meter
// leaves delegate unchanged. Instrument setup failures are also treated as a
// disabled measurement rather than affecting send behavior.
func NewPort(delegate applicationdelivery.Port, meter metric.Meter) applicationdelivery.Port {
	if delegate == nil || meter == nil {
		return delegate
	}

	return port{
		delegate: delegate,
		duration: newHistogram(
			meter,
			SendDurationMetricName,
			"Time spent sending a delivery message",
		),
	}
}

type claims struct {
	delegate applicationdelivery.Claims
	duration metric.Float64Histogram
}

var _ applicationdelivery.Claims = claims{}

func (claims claims) Claim(ctx context.Context) (applicationdelivery.Task, error) {
	started := time.Now()
	task, err := claims.delegate.Claim(ctx)

	outcome := claimOutcomeClaimed
	if err != nil {
		outcome = claimOutcomeError
		if errors.Is(err, applicationdelivery.ErrEmpty) {
			outcome = claimOutcomeEmpty
		}
	}
	record(claims.duration, ctx, started, outcome)

	return task, err
}

type port struct {
	delegate applicationdelivery.Port
	duration metric.Float64Histogram
}

var _ applicationdelivery.Port = port{}

func (port port) Send(
	ctx context.Context,
	recipient recipient.Recipient,
	message message.Message,
	count int64,
) error {
	started := time.Now()
	err := port.delegate.Send(ctx, recipient, message, count)

	outcome := sendOutcomeSuccess
	if err != nil {
		outcome = sendOutcomeError
		if errors.Is(err, context.Canceled) {
			outcome = sendOutcomeCanceled
		}
	}
	record(port.duration, ctx, started, outcome)

	return err
}

func newHistogram(meter metric.Meter, name, description string) metric.Float64Histogram {
	histogram, err := meter.Float64Histogram(
		name,
		metric.WithDescription(description),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil
	}
	return histogram
}

func record(
	histogram metric.Float64Histogram,
	ctx context.Context,
	started time.Time,
	outcome string,
) {
	if histogram == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	histogram.Record(
		ctx,
		time.Since(started).Seconds(),
		metric.WithAttributes(attribute.String(outcomeAttributeKey, outcome)),
	)
}
