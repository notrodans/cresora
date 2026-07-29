package delivery

import "testing"

func TestTerminalRunStatus(t *testing.T) {
	tests := []struct {
		name   string
		states []DeliveryState
		status RunTerminalStatus
		ready  bool
	}{
		{name: "all sent completes", states: []DeliveryState{DeliverySent, DeliverySent}, status: RunCompleted, ready: true},
		{name: "skipped is neutral", states: []DeliveryState{DeliverySent, DeliverySkipped}, status: RunCompleted, ready: true},
		{name: "failed fails", states: []DeliveryState{DeliverySent, DeliveryFailed}, status: RunFailed, ready: true},
		{name: "unknown fails", states: []DeliveryState{DeliverySent, DeliveryUnknown}, status: RunFailed, ready: true},
		{name: "unknown takes precedence while scanning", states: []DeliveryState{DeliveryUnknown, DeliveryFailed}, status: RunFailed, ready: true},
		{name: "pending is not terminal", states: []DeliveryState{DeliverySent, DeliveryPending}, ready: false},
		{name: "sending is not terminal", states: []DeliveryState{DeliverySent, DeliverySending}, ready: false},
		{name: "empty run is not terminal", ready: false},
		{name: "unrecognized state is not terminal", states: []DeliveryState{"future"}, ready: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, ready := TerminalRunStatus(test.states)
			if status != test.status || ready != test.ready {
				t.Fatalf("TerminalRunStatus(%v) = (%q, %t), want (%q, %t)", test.states, status, ready, test.status, test.ready)
			}
		})
	}
}
