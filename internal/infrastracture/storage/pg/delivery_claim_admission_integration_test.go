package pg_test

import (
	stdcontext "context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	application "github.com/notrodans/cresora/internal/application"
	applicationdelivery "github.com/notrodans/cresora/internal/application/commands/delivery"
	applicationoperatoraccounts "github.com/notrodans/cresora/internal/application/operatoraccounts"
	"github.com/notrodans/cresora/internal/domain/operatoraccount"
	pgclaims "github.com/notrodans/cresora/internal/infrastracture/storage/pg/claims"
	pgoperatoraccounts "github.com/notrodans/cresora/internal/infrastracture/storage/pg/operatoraccounts"
)

func TestPostgreSQLDeliveryClaimAccountAdmission(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	setupContext, cancel := stdcontext.WithTimeout(stdcontext.Background(), 60*time.Second)
	defer cancel()
	database, _, failure := newIsolatedDeliveryPipelineDatabase(setupContext, t, databaseURL)
	if failure != nil {
		t.Fatalf("prepare isolated PostgreSQL database: %v", failure)
	}

	t.Run("inactive account is not claimable", func(t *testing.T) {
		context, cancel := deliveryAdmissionContext()
		defer cancel()
		operatorID, accountID := createDeliveryAdmissionAccount(t, context, database)
		registerDeliveryAdmissionCleanup(t, database, operatorID)
		claims := pgclaims.NewClaims(database, time.Minute)

		_ = createLifecycleDelivery(t, context, database, operatorID, accountID)
		if _, failure := database.Exec(
			context,
			`UPDATE operator_accounts
			 SET status = 'disconnected', status_version = status_version + 1
			 WHERE id = $1`,
			accountID,
		); failure != nil {
			t.Fatalf("deactivate account: %v", failure)
		}

		task, claimFailure := claims.Claim(context)
		if !errors.Is(claimFailure, applicationdelivery.ErrEmpty) {
			t.Fatalf("claim inactive account error = %v, want %v", claimFailure, applicationdelivery.ErrEmpty)
		}
		if task != nil {
			t.Fatal("claim inactive account returned a task")
		}
	})

	t.Run("claimed admission remains fixed after account changes", func(t *testing.T) {
		context, cancel := deliveryAdmissionContext()
		defer cancel()
		operatorID, accountID := createDeliveryAdmissionAccount(t, context, database)
		registerDeliveryAdmissionCleanup(t, database, operatorID)
		claims := pgclaims.NewClaims(database, time.Minute)

		_ = createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim delivery: %v", claimFailure)
		}
		admitted, ok := task.(applicationdelivery.AdmittedTask)
		if !ok {
			t.Fatalf("claimed task type %T does not expose account admission", task)
		}
		admission := admitted.Admission()
		if admission.Route.UUID() != accountID || admission.Version == 0 {
			t.Fatalf("claimed admission = %+v, want route %s with a positive version", admission, accountID)
		}

		if _, failure := database.Exec(
			context,
			`UPDATE operator_accounts
			 SET status = 'active', status_version = status_version + 1
			 WHERE id = $1`,
			accountID,
		); failure != nil {
			t.Fatalf("advance account version: %v", failure)
		}
		if after := admitted.Admission(); after != admission {
			t.Fatalf("claimed admission changed after database update: before=%+v after=%+v", admission, after)
		}
	})

	t.Run("revalidation requires exact active version", func(t *testing.T) {
		context, cancel := deliveryAdmissionContext()
		defer cancel()
		operatorID, accountID := createDeliveryAdmissionAccount(t, context, database)
		registerDeliveryAdmissionCleanup(t, database, operatorID)
		claims := pgclaims.NewClaims(database, time.Minute)

		_ = createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("claim delivery for revalidation: %v", claimFailure)
		}
		admitted, ok := task.(applicationdelivery.AdmittedTask)
		if !ok {
			t.Fatalf("claimed task type %T does not expose account admission", task)
		}
		admission := admitted.Admission()

		target, revalidationFailure := claims.Revalidate(context, admission)
		if revalidationFailure != nil {
			t.Fatalf("revalidate current active account: %v", revalidationFailure)
		}
		if target.Actor.OperatorID != operatorID || target.AccountID.UUID() != accountID || target.Status != operatoraccount.StatusActive || target.Version != admission.Version {
			t.Fatalf("revalidated target = %+v, want operator %s account %s at admission version", target, operatorID, accountID)
		}

		if _, failure := database.Exec(
			context,
			`UPDATE operator_accounts
			 SET status = 'active', status_version = status_version + 1
			 WHERE id = $1`,
			accountID,
		); failure != nil {
			t.Fatalf("advance account version: %v", failure)
		}
		stale := admission
		if target, revalidationFailure = claims.Revalidate(context, stale); !errors.Is(revalidationFailure, applicationoperatoraccounts.ErrAccountNotFound) {
			t.Fatalf("revalidate stale admission error = %v, want %v", revalidationFailure, applicationoperatoraccounts.ErrAccountNotFound)
		} else if target != (applicationoperatoraccounts.RuntimeTarget{}) {
			t.Fatalf("revalidate stale admission target = %+v, want zero target", target)
		}

		current := admission
		current.Version++
		if target, revalidationFailure = claims.Revalidate(context, current); revalidationFailure != nil {
			t.Fatalf("revalidate current active version: %v", revalidationFailure)
		} else if target.Version != current.Version {
			t.Fatalf("revalidated current target version = %d, want %d", target.Version, current.Version)
		}

		if _, failure := database.Exec(
			context,
			`UPDATE operator_accounts
			 SET status = 'disconnected', status_version = status_version + 1
			 WHERE id = $1`,
			accountID,
		); failure != nil {
			t.Fatalf("deactivate account for revalidation: %v", failure)
		}
		inactive := current
		inactive.Version++
		if target, revalidationFailure = claims.Revalidate(context, inactive); !errors.Is(revalidationFailure, applicationoperatoraccounts.ErrAccountNotFound) {
			t.Fatalf("revalidate inactive account error = %v, want %v", revalidationFailure, applicationoperatoraccounts.ErrAccountNotFound)
		} else if target != (applicationoperatoraccounts.RuntimeTarget{}) {
			t.Fatalf("revalidate inactive account target = %+v, want zero target", target)
		}
	})

	t.Run("durable disconnect fences ready delivery admission", func(t *testing.T) {
		context, cancel := deliveryAdmissionContext()
		defer cancel()
		operatorID, accountID := createDeliveryAdmissionAccount(t, context, database)
		registerDeliveryAdmissionCleanup(t, database, operatorID)
		claims := pgclaims.NewClaims(database, time.Minute)

		_ = createLifecycleDelivery(t, context, database, operatorID, accountID)
		task, claimFailure := claims.Claim(context)
		if claimFailure != nil {
			t.Fatalf("capture ready delivery admission: %v", claimFailure)
		}
		admitted, ok := task.(applicationdelivery.AdmittedTask)
		if !ok {
			t.Fatalf("claimed task type %T does not expose account admission", task)
		}
		captured := admitted.Admission()
		remaining := createLifecycleDelivery(t, context, database, operatorID, accountID)
		assertReadyPendingDelivery(t, context, database, remaining)

		actor := application.Actor{OperatorID: operatorID}
		store := pgoperatoraccounts.New(database)
		account, loadFailure := store.LoadAccount(context, actor, operatoraccount.Identity(accountID))
		if loadFailure != nil {
			t.Fatalf("load active account before durable disconnect: %v", loadFailure)
		}
		if account.Version() != captured.Version || account.Status() != operatoraccount.StatusActive {
			t.Fatalf("active account = status %q version %d, want active version %d", account.Status(), account.Version(), captured.Version)
		}
		expectedVersion := account.Version()
		if failure := account.BeginDisconnect(); failure != nil {
			t.Fatalf("begin disconnect in domain: %v", failure)
		}
		if failure := store.PersistLifecycle(context, actor, account, expectedVersion); failure != nil {
			t.Fatalf("persist durable disconnect intent: %v", failure)
		}

		stored, loadFailure := store.LoadAccount(context, actor, operatoraccount.Identity(accountID))
		if loadFailure != nil {
			t.Fatalf("reload durable disconnect intent: %v", loadFailure)
		}
		if stored.Status() != operatoraccount.StatusDisconnecting || !stored.RemoteLogoutRequired() || stored.Version() != expectedVersion+1 {
			t.Fatalf("stored disconnect = status %q remote=%t version %d, want disconnecting remote=true version %d", stored.Status(), stored.RemoteLogoutRequired(), stored.Version(), expectedVersion+1)
		}

		assertReadyPendingDelivery(t, context, database, remaining)
		if task, claimFailure = claims.Claim(context); !errors.Is(claimFailure, applicationdelivery.ErrEmpty) {
			t.Fatalf("claim after durable disconnect error = %v, want %v", claimFailure, applicationdelivery.ErrEmpty)
		} else if task != nil {
			t.Fatal("claim after durable disconnect returned a task")
		}
		assertReadyPendingDelivery(t, context, database, remaining)

		if target, revalidationFailure := claims.Revalidate(context, captured); !errors.Is(revalidationFailure, applicationoperatoraccounts.ErrAccountNotFound) {
			t.Fatalf("revalidate captured active admission error = %v, want %v", revalidationFailure, applicationoperatoraccounts.ErrAccountNotFound)
		} else if target != (applicationoperatoraccounts.RuntimeTarget{}) {
			t.Fatalf("revalidate captured active admission target = %+v, want zero target", target)
		}
		current := captured
		current.Version = stored.Version()
		if target, revalidationFailure := claims.Revalidate(context, current); !errors.Is(revalidationFailure, applicationoperatoraccounts.ErrAccountNotFound) {
			t.Fatalf("revalidate disconnecting version error = %v, want %v", revalidationFailure, applicationoperatoraccounts.ErrAccountNotFound)
		} else if target != (applicationoperatoraccounts.RuntimeTarget{}) {
			t.Fatalf("revalidate disconnecting version target = %+v, want zero target", target)
		}
	})

	t.Run("route to another operator account is not claimable", func(t *testing.T) {
		context, cancel := deliveryAdmissionContext()
		defer cancel()
		ownerOperatorID, _ := createDeliveryAdmissionAccount(t, context, database)
		foreignOperatorID, foreignAccountID := createDeliveryAdmissionAccountWithUserID(t, context, database, 100010)
		registerDeliveryAdmissionCleanup(t, database, ownerOperatorID, foreignOperatorID)
		claims := pgclaims.NewClaims(database, time.Minute)

		_ = createLifecycleDelivery(t, context, database, ownerOperatorID, foreignAccountID)
		task, claimFailure := claims.Claim(context)
		if !errors.Is(claimFailure, applicationdelivery.ErrEmpty) {
			t.Fatalf("claim cross-operator route error = %v, want %v", claimFailure, applicationdelivery.ErrEmpty)
		}
		if task != nil {
			t.Fatal("claim cross-operator route returned a task")
		}
	})
}

func deliveryAdmissionContext() (stdcontext.Context, stdcontext.CancelFunc) {
	return stdcontext.WithTimeout(stdcontext.Background(), 60*time.Second)
}

func createDeliveryAdmissionAccount(
	t *testing.T,
	context stdcontext.Context,
	database *pgxpool.Pool,
) (uuid.UUID, uuid.UUID) {
	t.Helper()
	operatorID, accountID, failure := createLifecycleAccount(context, database)
	if failure != nil {
		t.Fatalf("create lifecycle account: %v", failure)
	}
	return operatorID, accountID
}

func createDeliveryAdmissionAccountWithUserID(
	t *testing.T,
	context stdcontext.Context,
	database *pgxpool.Pool,
	telegramUserID int64,
) (uuid.UUID, uuid.UUID) {
	t.Helper()
	operatorID := uuid.New()
	accountID := uuid.New()
	if _, failure := database.Exec(
		context,
		`INSERT INTO operators (id, username) VALUES ($1, $2)`,
		operatorID,
		"delivery-admission-"+operatorID.String(),
	); failure != nil {
		t.Fatalf("insert delivery admission operator: %v", failure)
	}
	if _, failure := database.Exec(
		context,
		`INSERT INTO operator_accounts
		 (id, operator_id, phone, telegram_username, telegram_first_name, telegram_user_id, status, status_version)
		 VALUES ($1, $2, $3, $4, $5, $6, 'active', 1)`,
		accountID,
		operatorID,
		"+19990000009",
		"delivery_"+accountID.String()[:8],
		"Delivery Test",
		telegramUserID,
	); failure != nil {
		t.Fatalf("insert delivery admission account: %v", failure)
	}
	return operatorID, accountID
}

func registerDeliveryAdmissionCleanup(t *testing.T, database *pgxpool.Pool, operatorIDs ...uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		for _, operatorID := range operatorIDs {
			if failure := cleanupDeliveryPipelineFixture(stdcontext.Background(), database, operatorID); failure != nil {
				t.Errorf("cleanup delivery admission operator %s: %v", operatorID, failure)
			}
		}
	})
}

func assertReadyPendingDelivery(
	t *testing.T,
	context stdcontext.Context,
	database *pgxpool.Pool,
	item lifecycleDelivery,
) {
	t.Helper()
	var (
		status string
		ready  bool
	)
	if failure := database.QueryRow(
		context,
		`SELECT status::text, ready_at <= CURRENT_TIMESTAMP
		 FROM mailing_deliveries
		 WHERE mailing_id = $1 AND run_id = $2 AND recipient_id = $3`,
		item.mailingID,
		item.runID,
		item.recipientID,
	).Scan(&status, &ready); failure != nil {
		t.Fatalf("read remaining delivery: %v", failure)
	}
	if status != "pending" || !ready {
		t.Fatalf("remaining delivery = status %q ready=%t, want pending and ready", status, ready)
	}
}
