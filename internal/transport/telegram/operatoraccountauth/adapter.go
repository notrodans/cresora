// Package operatoraccountauth is the Telegram transport boundary for
// operator-account phone authentication.
package operatoraccountauth

import (
	"context"
	"errors"
	"time"

	gotdtelegram "github.com/gotd/td/telegram"
	gotdauth "github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	application "github.com/notrodans/cresora/internal/application/operatoraccountauth"
	"github.com/notrodans/cresora/internal/transport/telegram/accountowner"
)

// Runtime admits one callback against the account runtime.
type Runtime interface {
	Execute(context.Context, application.AuthTarget, accountowner.ClientCallback) error
}

// Adapter implements the application phone-auth provider. Every public method
// performs one independent runtime operation and keeps all gotd values inside
// this transport package.
type Adapter struct {
	runtime       Runtime
	clientFactory func(*gotdtelegram.Client) phoneClient
}

var _ application.PhoneProvider = Adapter{}
var _ Runtime = (*accountowner.Registry)(nil)

// New constructs a provider backed by the account runtime. It does not start
// a client or perform network I/O.
func New(runtime Runtime) Adapter {
	return Adapter{
		runtime:       runtime,
		clientFactory: newGotdPhoneClient,
	}
}

// SendCode sends one Telegram phone code and returns only its opaque
// coordinator hash and safe challenge metadata.
func (adapter Adapter) SendCode(
	ctx context.Context,
	target application.AuthTarget,
	phone string,
) (application.SendCodeResult, error) {
	var sent sentCode
	failure := adapter.runtime.Execute(ctx, target, func(callbackContext context.Context, raw *gotdtelegram.Client) error {
		client := adapter.clientFactory(raw)
		if client == nil {
			return application.ErrProviderUnavailable
		}
		var err error
		sent, err = client.sendCode(callbackContext, phone)
		return err
	})
	if failure != nil {
		return application.SendCodeResult{}, mapProviderError(failure)
	}
	if sent.hash == "" {
		return application.SendCodeResult{}, newProviderFailure(application.ProviderFailureProtocol)
	}
	return application.SendCodeResult{
		PhoneCodeHash: application.NewPhoneCodeHash(sent.hash),
		Delivery:      sent.delivery,
		ExpiresAt:     sent.expiresAt,
	}, nil
}

// SignIn submits one phone code and, only after successful sign-in, fetches
// Self in the same runtime callback.
func (adapter Adapter) SignIn(
	ctx context.Context,
	target application.AuthTarget,
	phone string,
	code string,
	hash application.PhoneCodeHash,
) (application.Profile, error) {
	var profile application.Profile
	failure := adapter.runtime.Execute(ctx, target, func(callbackContext context.Context, raw *gotdtelegram.Client) error {
		client := adapter.clientFactory(raw)
		if client == nil {
			return application.ErrProviderUnavailable
		}
		if err := client.signIn(callbackContext, phone, code, hash.Value()); err != nil {
			return err
		}
		var err error
		profile, err = client.self(callbackContext)
		return err
	})
	if failure != nil {
		return application.Profile{}, mapProviderError(failure)
	}
	if profile.UserID <= 0 {
		return application.Profile{}, application.ErrSessionUnavailable
	}
	return profile, nil
}

// Password submits one Telegram 2FA password and then fetches Self in the
// same runtime callback.
func (adapter Adapter) Password(
	ctx context.Context,
	target application.AuthTarget,
	password string,
) (application.Profile, error) {
	var profile application.Profile
	failure := adapter.runtime.Execute(ctx, target, func(callbackContext context.Context, raw *gotdtelegram.Client) error {
		client := adapter.clientFactory(raw)
		if client == nil {
			return application.ErrProviderUnavailable
		}
		if err := client.password(callbackContext, password); err != nil {
			return err
		}
		var err error
		profile, err = client.self(callbackContext)
		return err
	})
	if failure != nil {
		return application.Profile{}, mapProviderError(failure)
	}
	if profile.UserID <= 0 {
		return application.Profile{}, application.ErrSessionUnavailable
	}
	return profile, nil
}

type sentCode struct {
	hash      string
	delivery  string
	expiresAt time.Time
}

// phoneClient is deliberately transport-local. It lets provider tests use a
// deterministic fake without constructing a gotd client or making a network
// request, while gotd request and response types remain private to this file.
type phoneClient interface {
	sendCode(context.Context, string) (sentCode, error)
	signIn(context.Context, string, string, string) error
	password(context.Context, string) error
	self(context.Context) (application.Profile, error)
}

type gotdPhoneClient struct {
	client *gotdtelegram.Client
}

func newGotdPhoneClient(client *gotdtelegram.Client) phoneClient {
	if client == nil {
		return nil
	}
	return gotdPhoneClient{client: client}
}

func (client gotdPhoneClient) sendCode(ctx context.Context, phone string) (sentCode, error) {
	result, err := client.client.Auth().SendCode(ctx, phone, gotdauth.SendCodeOptions{})
	if err != nil {
		return sentCode{}, err
	}
	sent, ok := result.(*tg.AuthSentCode)
	if !ok || sent == nil || sent.PhoneCodeHash == "" {
		return sentCode{}, errInvalidSendCodeResponse
	}
	return sentCode{
		hash:      sent.PhoneCodeHash,
		delivery:  delivery(sent.Type),
		expiresAt: codeExpiry(sent.Timeout),
	}, nil
}

func (client gotdPhoneClient) signIn(ctx context.Context, phone, code, hash string) error {
	_, err := client.client.Auth().SignIn(ctx, phone, code, hash)
	return err
}

func (client gotdPhoneClient) password(ctx context.Context, password string) error {
	_, err := client.client.Auth().Password(ctx, password)
	return err
}

func (client gotdPhoneClient) self(ctx context.Context) (application.Profile, error) {
	user, err := client.client.Self(ctx)
	if err != nil {
		return application.Profile{}, err
	}
	return profile(user)
}

func delivery(codeType tg.AuthSentCodeTypeClass) string {
	switch codeType.(type) {
	case *tg.AuthSentCodeTypeApp:
		return "APP"
	case *tg.AuthSentCodeTypeSMS:
		return "SMS"
	case *tg.AuthSentCodeTypeCall:
		return "CALL"
	case *tg.AuthSentCodeTypeFlashCall:
		return "FLASH_CALL"
	case *tg.AuthSentCodeTypeMissedCall:
		return "MISSED_CALL"
	case *tg.AuthSentCodeTypeEmailCode:
		return "EMAIL"
	case *tg.AuthSentCodeTypeFragmentSMS, *tg.AuthSentCodeTypeFirebaseSMS,
		*tg.AuthSentCodeTypeSMSWord, *tg.AuthSentCodeTypeSMSPhrase:
		return "SMS"
	default:
		return "Telegram code"
	}
}

func codeExpiry(timeout int) time.Time {
	if timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(time.Duration(timeout) * time.Second)
}

func profile(user *tg.User) (application.Profile, error) {
	if user == nil || user.ID <= 0 {
		return application.Profile{}, application.ErrSessionUnavailable
	}
	return application.Profile{
		UserID:    user.ID,
		Username:  user.Username,
		FirstName: user.FirstName,
		LastName:  user.LastName,
	}, nil
}

func mapProviderError(failure error) error {
	if failure == nil {
		return nil
	}
	if errors.Is(failure, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(failure, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if retry, ok := errors.AsType[*application.RetryAfterError](failure); ok {
		return retry
	}
	if errors.Is(failure, accountowner.ErrRegistryStopped) || errors.Is(failure, accountowner.ErrAccountStopped) {
		return application.ErrProviderUnavailable
	}
	if errors.Is(failure, accountowner.ErrRuntimeCapacity) {
		return newProviderFailure(application.ProviderFailureRuntimeCapacity)
	}
	if errors.Is(failure, accountowner.ErrStaleAdmission) || errors.Is(failure, accountowner.ErrInvalidAdmission) {
		return application.ErrSessionUnavailable
	}
	var providerFailure *application.ProviderFailureError
	if errors.As(failure, &providerFailure) && providerFailure != nil {
		return newProviderFailure(providerFailure.Kind())
	}
	for _, approved := range []error{
		application.ErrInvalidCode,
		application.ErrPasswordRequired,
		application.ErrInvalidPassword,
		application.ErrProviderUnavailable,
		application.ErrProviderTransient,
		application.ErrUnauthorized,
		application.ErrSessionUnavailable,
		application.ErrFloodWait,
	} {
		if errors.Is(failure, approved) {
			return approved
		}
	}
	if after, ok := tgerr.AsFloodWait(failure); ok {
		safe, err := application.NewRetryAfterError(after)
		if err == nil {
			return safe
		}
	}
	if errors.Is(failure, gotdauth.ErrPasswordAuthNeeded) || tgerr.Is(failure, "SESSION_PASSWORD_NEEDED") {
		return application.ErrPasswordRequired
	}
	if errors.Is(failure, gotdauth.ErrPasswordInvalid) || tgerr.Is(failure, "PASSWORD_HASH_INVALID") {
		return application.ErrInvalidPassword
	}
	if tgerr.Is(failure, "API_ID_INVALID") {
		return newProviderFailure(application.ProviderFailureConfigurationRejected)
	}
	if tgerr.Is(failure, "PHONE_NUMBER_INVALID", "PHONE_NUMBER_BANNED", "PHONE_NUMBER_FLOOD") {
		return newProviderFailure(application.ProviderFailurePhoneRejected)
	}
	if tgerr.Is(failure,
		"PHONE_CODE_EMPTY",
		"PHONE_CODE_INVALID",
		"PHONE_CODE_EXPIRED",
		"PHONE_CODE_HASH_EMPTY",
		"PHONE_CODE_HASH_INVALID",
	) {
		return application.ErrInvalidCode
	}
	if tgerr.Is(failure,
		"AUTH_KEY_UNREGISTERED",
		"AUTH_KEY_INVALID",
		"SESSION_REVOKED",
		"SESSION_EXPIRED",
		"AUTH_KEY_DUPLICATED",
	) {
		return application.ErrSessionUnavailable
	}
	if gotdauth.IsUnauthorized(failure) || tgerr.Is(failure, "UNAUTHORIZED") {
		return application.ErrUnauthorized
	}
	if errors.Is(failure, errInvalidSendCodeResponse) {
		return newProviderFailure(application.ProviderFailureProtocol)
	}
	if rpcFailure, ok := tgerr.As(failure); ok {
		switch {
		case rpcFailure.Code >= 400 && rpcFailure.Code < 500:
			return newProviderFailure(application.ProviderFailureRemoteRejected)
		case rpcFailure.Code >= 500 && rpcFailure.Code < 600:
			return newProviderFailure(application.ProviderFailureRemoteFailure)
		}
	}
	return newProviderFailure(application.ProviderFailureTransportUnknown)
}

func newProviderFailure(kind application.ProviderFailureKind) error {
	failure, err := application.NewProviderFailureError(kind)
	if err != nil {
		return application.ErrProviderTransient
	}
	return failure
}

var errInvalidSendCodeResponse = errors.New("telegram authentication send-code response is invalid")
