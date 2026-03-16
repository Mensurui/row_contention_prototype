package ledger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

func TestProcessPoolWinConcurrentWithContributions(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL or DATABASE_URL to run integration tests")
	}

	ctx := context.Background()
	testPool, cleanup := newTestPool(ctx, t, databaseURL)
	defer cleanup()

	service := NewService(testPool)
	pool, err := service.CreatePool(ctx, "weekly-ekub", decimal.RequireFromString("100.0000"))
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}

	customers := make([]Customer, 50)
	for i := range customers {
		customers[i], err = service.CreateCustomer(ctx, fmt.Sprintf("customer-%02d", i+1))
		if err != nil {
			t.Fatalf("create customer %d: %v", i+1, err)
		}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var winSucceeded atomic.Bool
	errCh := make(chan error, len(customers)+1)

	for i := range customers {
		customer := customers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := service.ContributeToPool(ctx, pool.ID, customer.ID, pool.ContributionAmount)
			if err != nil {
				errCh <- err
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			_, _, err := service.ProcessPoolWin(ctx, pool.ID, customers[0].ID)
			if err == nil {
				winSucceeded.Store(true)
				return
			}
			if !errors.Is(err, ErrInsufficientPoolBalance) {
				errCh <- err
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		errCh <- errors.New("process pool win did not succeed before deadline")
	}()

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent execution failed: %v", err)
		}
	}

	if !winSucceeded.Load() {
		t.Fatal("expected ProcessPoolWin to succeed")
	}

	total, err := service.GetLedgerTotal(ctx)
	if err != nil {
		t.Fatalf("get ledger total: %v", err)
	}
	if !total.Equal(zeroAmount) {
		t.Fatalf("ledger total = %s, want 0.0000", total.StringFixed(4))
	}
}

func newTestPool(ctx context.Context, t *testing.T, databaseURL string) (*pgxpool.Pool, func()) {
	t.Helper()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}

	schemaName := "ledger_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	config.ConnConfig.RuntimeParams["search_path"] = schemaName
	config.MaxConns = 16

	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse admin database url: %v", err)
	}

	adminPool, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}

	if _, err := adminPool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA "%s"`, schemaName)); err != nil {
		adminPool.Close()
		t.Fatalf("create schema: %v", err)
	}

	testPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		adminPool.Close()
		t.Fatalf("connect test pool: %v", err)
	}

	migrationPath := filepath.Join("..", "migrations", "00001_init_ledger.sql")
	if err := applyMigration(ctx, testPool, migrationPath); err != nil {
		testPool.Close()
		_, _ = adminPool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, schemaName))
		adminPool.Close()
		t.Fatalf("apply migration: %v", err)
	}

	cleanup := func() {
		testPool.Close()
		_, _ = adminPool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, schemaName))
		adminPool.Close()
	}

	return testPool, cleanup
}

func applyMigration(ctx context.Context, pool *pgxpool.Pool, migrationPath string) error {
	contents, err := os.ReadFile(migrationPath)
	if err != nil {
		return fmt.Errorf("read migration file: %w", err)
	}

	upStatements := ExtractGooseUpStatements(string(contents))
	if upStatements == "" {
		return errors.New("migration file does not contain goose up statements")
	}

	if _, err := pool.Exec(ctx, upStatements); err != nil {
		return fmt.Errorf("execute migration: %w", err)
	}

	var totalTables int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema()`).Scan(&totalTables); err != nil {
		return fmt.Errorf("verify migration tables: %w", err)
	}
	if totalTables < 4 {
		return fmt.Errorf("expected migration to create at least 4 tables, found %d", totalTables)
	}

	return nil
}
