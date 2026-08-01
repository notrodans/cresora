package account

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	gotdtelegram "github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	delivery "github.com/notrodans/cresora/internal/application/commands/delivery"
	"github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/mailing"
	"github.com/notrodans/cresora/internal/domain/message"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	"github.com/notrodans/cresora/internal/domain/recipient"
	"github.com/notrodans/cresora/internal/transport/telegram"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
)

func TestAdmissionCommandsRejectStaleRevalidationBeforeRuntime(t *testing.T) {
	reader := &revalidationFake{failure: fmt.Errorf("account disappeared: %w", operatoraccounts.ErrAccountNotFound)}
	runtime := &runtimeFake{}
	commands := newTestCommands(reader, runtime, &telegramAPI{})

	_, failure := commands.Command(context.Background(), testAdmission())
	if !errors.Is(failure, delivery.ErrAccountAdmissionRejected) {
		t.Fatalf("Command() error = %v, want ErrAccountAdmissionRejected", failure)
	}
	if !errors.Is(failure, operatoraccounts.ErrAccountNotFound) {
		t.Fatalf("Command() error = %v, want original stale/inactive error", failure)
	}
	if got := reader.calls(); got != 1 {
		t.Fatalf("Revalidate() calls = %d, want 1", got)
	}
	if got := runtime.executeCount(); got != 0 {
		t.Fatalf("Runtime.Execute() calls = %d, want 0", got)
	}
}

func TestRuntimeGatedPortStoppedBeforeCallbackIsTransient(t *testing.T) {
	runtime := &runtimeFake{failure: accountowner.ErrAccountStopped}
	api := &telegramAPI{}
	commands := newTestCommands(&revalidationFake{target: testTarget()}, runtime, api)
	command := resolveTestCommand(t, commands)

	failure := executeTestCommand(t, command)
	if !errors.Is(failure, delivery.ErrTransient) {
		t.Fatalf("Execute() error = %v, want delivery.ErrTransient", failure)
	}
	if !errors.Is(failure, accountowner.ErrAccountStopped) {
		t.Fatalf("Execute() error = %v, want ErrAccountStopped", failure)
	}
	if got := runtime.executeCount(); got != 1 {
		t.Fatalf("Runtime.Execute() calls = %d, want 1", got)
	}
	if got := runtime.callbackCount(); got != 0 {
		t.Fatalf("runtime callback calls = %d, want 0", got)
	}
	if got := api.callsCount(); got != 0 {
		t.Fatalf("Telegram RPC calls = %d, want 0", got)
	}
}

func TestRuntimeGatedPortMissingCallbackIsUnknownFailure(t *testing.T) {
	runtime := &runtimeFake{}
	api := &telegramAPI{}
	commands := newTestCommands(&revalidationFake{target: testTarget()}, runtime, api)
	command := resolveTestCommand(t, commands)

	failure := executeTestCommand(t, command)
	if failure == nil {
		t.Fatal("Execute() returned nil without a runtime callback")
	}
	if errors.Is(failure, delivery.ErrTransient) {
		t.Fatalf("Execute() error = %v, must not be transient", failure)
	}
	if !errors.Is(failure, delivery.ErrUnknownOutcome) {
		t.Fatalf("Execute() error = %v, want ErrUnknownOutcome", failure)
	}
	if got := runtime.executeCount(); got != 1 {
		t.Fatalf("Runtime.Execute() calls = %d, want 1", got)
	}
	if got := runtime.callbackCount(); got != 0 {
		t.Fatalf("runtime callback calls = %d, want 0", got)
	}
	if got := api.callsCount(); got != 0 {
		t.Fatalf("Telegram RPC calls = %d, want 0", got)
	}
}

func TestRuntimeGatedPortValidPathUsesOneCallbackAndPersistedRandomID(t *testing.T) {
	const randomID int64 = 918273
	runtime := &runtimeFake{invoke: true}
	api := &telegramAPI{}
	commands := newTestCommands(&revalidationFake{target: testTarget()}, runtime, api)
	commands.deliveries = testDeliveries(randomID, message.Text("hello"))
	command := resolveTestCommand(t, commands)

	if failure := executeTestCommand(t, command); failure != nil {
		t.Fatalf("Execute() error = %v, want nil", failure)
	}
	if got := runtime.executeCount(); got != 1 {
		t.Fatalf("Runtime.Execute() calls = %d, want 1", got)
	}
	if got := runtime.callbackCount(); got != 1 {
		t.Fatalf("runtime callback calls = %d, want 1", got)
	}
	if got := api.callsCount(); got != 1 {
		t.Fatalf("Telegram RPC calls = %d, want 1", got)
	}
	if got := api.randomID(); got != randomID {
		t.Fatalf("Telegram RandomID = %d, want persisted %d", got, randomID)
	}
}

func TestRuntimeGatedPortPreservesFloodWaitDuration(t *testing.T) {
	const wantDuration = 7 * time.Second
	runtime := &runtimeFake{invoke: true}
	api := &telegramAPI{failure: tgerr.New(420, "FLOOD_WAIT_7")}
	commands := newTestCommands(&revalidationFake{target: testTarget()}, runtime, api)
	command := resolveTestCommand(t, commands)

	failure := executeTestCommand(t, command)
	var floodWait *delivery.FloodWaitError
	if !errors.As(failure, &floodWait) || floodWait == nil {
		t.Fatalf("Execute() error = %v, want FloodWaitError", failure)
	}
	if floodWait.Duration != wantDuration {
		t.Fatalf("FloodWait duration = %s, want %s", floodWait.Duration, wantDuration)
	}
	if got := runtime.callbackCount(); got != 1 {
		t.Fatalf("runtime callback calls = %d, want 1", got)
	}
}

func newTestCommands(
	reader *revalidationFake,
	runtime *runtimeFake,
	api *telegramAPI,
) admissionCommands {
	return newAdmissionCommands(
		reader,
		testDeliveries(123, message.Text("test")),
		targetsFake{},
		runtime,
		func(*gotdtelegram.Client) telegram.API { return api },
	)
}

func resolveTestCommand(t *testing.T, commands admissionCommands) delivery.Command {
	t.Helper()
	command, failure := commands.Command(context.Background(), testAdmission())
	if failure != nil {
		t.Fatalf("Command() error = %v, want nil", failure)
	}
	return command
}

func executeTestCommand(t *testing.T, command delivery.Command) error {
	t.Helper()
	return command.Execute(
		context.Background(),
		mailing.Identity(uuid.New()),
		mailing.Run(uuid.New()),
		recipient.Identifier(uuid.New()),
		delivery.Fence(uuid.New()),
	)
}

func testAdmission() delivery.AccountAdmission {
	return delivery.AccountAdmission{
		Route:   delivery.Routing(uuid.MustParse("11111111-1111-1111-1111-111111111111")),
		Version: operatoraccount.Version(4),
	}
}

func testTarget() operatoraccounts.RuntimeTarget {
	admission := testAdmission()
	return operatoraccounts.RuntimeTarget{
		AccountID: operatoraccount.Identity(admission.Route.UUID()),
		Status:    operatoraccount.StatusActive,
		Version:   admission.Version,
	}
}

type revalidationFake struct {
	mu      sync.Mutex
	target  operatoraccounts.RuntimeTarget
	failure error
	count   int
}

func (fake *revalidationFake) Revalidate(
	_ context.Context,
	_ delivery.AccountAdmission,
) (operatoraccounts.RuntimeTarget, error) {
	fake.mu.Lock()
	fake.count++
	fake.mu.Unlock()
	return fake.target, fake.failure
}

func (fake *revalidationFake) calls() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.count
}

type runtimeFake struct {
	mu            sync.Mutex
	failure       error
	invoke        bool
	executeCalls  int
	callbackCalls int
}

func (fake *runtimeFake) Execute(
	context context.Context,
	_ operatoraccounts.RuntimeTarget,
	callback accountowner.ClientCallback,
) error {
	fake.mu.Lock()
	fake.executeCalls++
	invoke := fake.invoke
	failure := fake.failure
	if invoke {
		fake.callbackCalls++
	}
	fake.mu.Unlock()
	if failure != nil {
		return failure
	}
	if !invoke {
		return nil
	}
	return callback(context, nil)
}

func (fake *runtimeFake) executeCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.executeCalls
}

func (fake *runtimeFake) callbackCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.callbackCalls
}

type deliveriesFake struct {
	recipient recipient.Recipient
	message   message.Message
	randomID  int64
}

func testDeliveries(randomID int64, body message.Message) delivery.Deliveries {
	return deliveriesFake{
		recipient: recipient.Identity(uuid.New()),
		message:   body,
		randomID:  randomID,
	}
}

func (deliveries deliveriesFake) Delivery(
	mailing.ID,
	mailing.RunID,
	recipient.ID,
	delivery.Token,
) delivery.Delivery {
	return deliveryDispatch{
		recipient: deliveries.recipient,
		message:   deliveries.message,
		randomID:  deliveries.randomID,
	}
}

type deliveryDispatch struct {
	recipient recipient.Recipient
	message   message.Message
	randomID  int64
}

func (dispatch deliveryDispatch) Dispatch(ctx context.Context, port delivery.Port) error {
	return port.Send(ctx, dispatch.recipient, dispatch.message, dispatch.randomID)
}

type targetsFake struct{}

func (targetsFake) Targets(delivery.Route) telegram.Targets {
	return targetSetFake{}
}

type targetSetFake struct{}

func (targetSetFake) Target(_ context.Context, _ recipient.Recipient) (telegram.Target, error) {
	return targetFake{}, nil
}

type targetFake struct{}

func (targetFake) Peer() (tg.InputPeerClass, error) {
	return &tg.InputPeerUser{UserID: 1, AccessHash: 2}, nil
}

type telegramAPI struct {
	mu      sync.Mutex
	failure error
	calls   int
	lastID  int64
}

func (api *telegramAPI) MessagesSendMessage(
	_ context.Context,
	request *tg.MessagesSendMessageRequest,
) (tg.UpdatesClass, error) {
	api.mu.Lock()
	api.calls++
	api.lastID = request.RandomID
	failure := api.failure
	api.mu.Unlock()
	return nil, failure
}

func (api *telegramAPI) callsCount() int {
	api.mu.Lock()
	defer api.mu.Unlock()
	return api.calls
}

func (api *telegramAPI) randomIDValue() int64 {
	api.mu.Lock()
	defer api.mu.Unlock()
	return api.lastID
}

func (api *telegramAPI) randomID() int64 {
	return api.randomIDValue()
}
