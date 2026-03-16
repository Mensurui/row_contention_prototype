package ledger

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

var (
	ErrPoolNotFound            = errors.New("pool not found")
	ErrCustomerNotFound        = errors.New("customer not found")
	ErrInsufficientPoolBalance = errors.New("pool balance is zero")
	ErrUnbalancedLedgerEntries = errors.New("ledger entries do not balance")
	defaultTreasuryAccountID   = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	treasuryRevenueShare       = decimal.RequireFromString("0.0500")
	zeroAmount                 = decimal.RequireFromString("0.0000")
)

type LedgerService interface {
	CreateCustomer(ctx context.Context, name string) (Customer, error)
	CreatePool(ctx context.Context, name string, contributionAmount decimal.Decimal) (Pool, error)
	ContributeToPool(ctx context.Context, poolID, customerID uuid.UUID, amount decimal.Decimal) (Transaction, []LedgerEntry, error)
	ProcessPoolWin(ctx context.Context, poolID, winnerID uuid.UUID) (Transaction, []LedgerEntry, error)
	GetAccountBalance(ctx context.Context, accountID uuid.UUID) (decimal.Decimal, error)
	GetLedgerTotal(ctx context.Context) (decimal.Decimal, error)
}

type Service struct {
	pool              *pgxpool.Pool
	treasuryAccountID uuid.UUID
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{
		pool:              pool,
		treasuryAccountID: defaultTreasuryAccountID,
	}
}

func (s *Service) CreateCustomer(ctx context.Context, name string) (Customer, error) {
	const query = `
		INSERT INTO customers (id, account_id, name)
		VALUES ($1, $2, $3)
		RETURNING created_at`

	customer := Customer{
		ID:        uuid.New(),
		AccountID: uuid.New(),
		Name:      name,
	}

	if err := s.pool.QueryRow(ctx, query, customer.ID, customer.AccountID, customer.Name).Scan(&customer.CreatedAt); err != nil {
		return Customer{}, fmt.Errorf("create customer: %w", err)
	}

	return customer, nil
}

func (s *Service) CreatePool(ctx context.Context, name string, contributionAmount decimal.Decimal) (Pool, error) {
	const query = `
		INSERT INTO pools (id, account_id, name, contribution_amount)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at`

	pool := Pool{
		ID:                 uuid.New(),
		AccountID:          uuid.New(),
		Name:               name,
		ContributionAmount: normalizeAmount(contributionAmount),
	}

	if !pool.ContributionAmount.IsPositive() {
		return Pool{}, fmt.Errorf("create pool: contribution amount must be positive")
	}

	if err := s.pool.QueryRow(ctx, query, pool.ID, pool.AccountID, pool.Name, pool.ContributionAmount.StringFixed(4)).Scan(&pool.CreatedAt); err != nil {
		return Pool{}, fmt.Errorf("create pool: %w", err)
	}

	return pool, nil
}

func (s *Service) ContributeToPool(ctx context.Context, poolID, customerID uuid.UUID, amount decimal.Decimal) (txRecord Transaction, entries []LedgerEntry, err error) {
	normalizedAmount := normalizeAmount(amount)
	if !normalizedAmount.IsPositive() {
		return Transaction{}, nil, fmt.Errorf("contribute to pool: amount must be positive")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("contribute to pool: begin tx: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if err = lockPool(ctx, tx, poolID); err != nil {
		return Transaction{}, nil, fmt.Errorf("contribute to pool: lock pool: %w", err)
	}

	poolAccountID, err := fetchPoolAccountID(ctx, tx, poolID)
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("contribute to pool: %w", err)
	}

	customerAccountID, err := fetchCustomerAccountID(ctx, tx, customerID)
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("contribute to pool: %w", err)
	}

	txRecord, entries, err = createLedgerTransaction(
		ctx,
		tx,
		&poolID,
		"pool_contribution",
		fmt.Sprintf("Contribution to pool %s by customer %s", poolID, customerID),
		[]ledgerEntryInput{
			{AccountID: poolAccountID, Amount: normalizedAmount},
			{AccountID: customerAccountID, Amount: normalizedAmount.Neg()},
		},
	)
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("contribute to pool: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return Transaction{}, nil, fmt.Errorf("contribute to pool: commit tx: %w", err)
	}

	return txRecord, entries, nil
}

func (s *Service) ProcessPoolWin(ctx context.Context, poolID, winnerID uuid.UUID) (txRecord Transaction, entries []LedgerEntry, err error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("process pool win: begin tx: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// since we will be having only one winner per pool
	if err = lockPool(ctx, tx, poolID); err != nil {
		return Transaction{}, nil, fmt.Errorf("process pool win: lock pool: %w", err)
	}

	poolAccountID, err := fetchPoolAccountID(ctx, tx, poolID)
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("process pool win: %w", err)
	}

	// application level decision on who the winner is will be made then that customers account will be fetched here
	winnerAccountID, err := fetchCustomerAccountID(ctx, tx, winnerID)
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("process pool win: %w", err)
	}

	poolBalance, err := fetchAccountBalance(ctx, tx, poolAccountID)
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("process pool win: fetch pool balance: %w", err)
	}

	// this logically shouldb't happen
	if !poolBalance.IsPositive() {
		return Transaction{}, nil, ErrInsufficientPoolBalance
	}

	treasuryAmount := normalizeAmount(poolBalance.Mul(treasuryRevenueShare))
	winnerAmount := normalizeAmount(poolBalance.Sub(treasuryAmount))

	txRecord, entries, err = createLedgerTransaction(
		ctx,
		tx,
		&poolID,
		"pool_win",
		fmt.Sprintf("Pool win payout for pool %s to winner %s", poolID, winnerID),
		[]ledgerEntryInput{
			// the pool balance is negative because -poolbalance + winneramount + treasureamoun == 0 every time
			{AccountID: poolAccountID, Amount: poolBalance.Neg()},
			{AccountID: winnerAccountID, Amount: winnerAmount},
			{AccountID: s.treasuryAccountID, Amount: treasuryAmount},
		},
	)
	if err != nil {
		return Transaction{}, nil, fmt.Errorf("process pool win: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return Transaction{}, nil, fmt.Errorf("process pool win: commit tx: %w", err)
	}

	return txRecord, entries, nil
}

func (s *Service) GetAccountBalance(ctx context.Context, accountID uuid.UUID) (decimal.Decimal, error) {
	balance, err := fetchAccountBalance(ctx, s.pool, accountID)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("get account balance: %w", err)
	}

	return balance, nil
}

func (s *Service) GetLedgerTotal(ctx context.Context) (decimal.Decimal, error) {
	const query = `SELECT COALESCE(SUM(amount)::text, '0.0000') FROM ledger_entries`

	var totalText string
	if err := s.pool.QueryRow(ctx, query).Scan(&totalText); err != nil {
		return decimal.Decimal{}, fmt.Errorf("get ledger total: %w", err)
	}

	total, err := decimal.NewFromString(totalText)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("get ledger total: parse decimal: %w", err)
	}

	return normalizeAmount(total), nil
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type ledgerEntryInput struct {
	AccountID uuid.UUID
	Amount    decimal.Decimal
}

func createLedgerTransaction(ctx context.Context, tx pgx.Tx, poolID *uuid.UUID, txType, description string, inputs []ledgerEntryInput) (Transaction, []LedgerEntry, error) {
	total := decimal.Zero
	for _, input := range inputs {
		total = total.Add(normalizeAmount(input.Amount))
	}

	if !normalizeAmount(total).Equal(zeroAmount) {
		return Transaction{}, nil, ErrUnbalancedLedgerEntries
	}

	record := Transaction{
		ID:          uuid.New(),
		PoolID:      poolID,
		Type:        txType,
		Description: description,
	}

	const transactionInsert = `
		INSERT INTO transactions (id, pool_id, type, description)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at`

	if err := tx.QueryRow(ctx, transactionInsert, record.ID, poolID, txType, description).Scan(&record.CreatedAt); err != nil {
		return Transaction{}, nil, fmt.Errorf("insert transaction: %w", err)
	}

	entries := make([]LedgerEntry, 0, len(inputs))
	const entryInsert = `
		INSERT INTO ledger_entries (id, transaction_id, account_id, amount)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at`

	for _, input := range inputs {
		entry := LedgerEntry{
			ID:            uuid.New(),
			TransactionID: record.ID,
			AccountID:     input.AccountID,
			Amount:        normalizeAmount(input.Amount),
		}

		if err := tx.QueryRow(ctx, entryInsert, entry.ID, entry.TransactionID, entry.AccountID, entry.Amount.StringFixed(4)).Scan(&entry.CreatedAt); err != nil {
			return Transaction{}, nil, fmt.Errorf("insert ledger entry for account %s: %w", entry.AccountID, err)
		}

		entries = append(entries, entry)
	}

	return record, entries, nil
}

func lockPool(ctx context.Context, tx pgx.Tx, poolID uuid.UUID) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1::text))`, poolID.String())
	return err
}

func fetchPoolAccountID(ctx context.Context, rower queryRower, poolID uuid.UUID) (uuid.UUID, error) {
	const query = `SELECT account_id FROM pools WHERE id = $1`

	var accountID uuid.UUID
	if err := rower.QueryRow(ctx, query, poolID).Scan(&accountID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.UUID{}, ErrPoolNotFound
		}
		return uuid.UUID{}, fmt.Errorf("fetch pool account: %w", err)
	}

	return accountID, nil
}

func fetchCustomerAccountID(ctx context.Context, rower queryRower, customerID uuid.UUID) (uuid.UUID, error) {
	const query = `SELECT account_id FROM customers WHERE id = $1`

	var accountID uuid.UUID
	if err := rower.QueryRow(ctx, query, customerID).Scan(&accountID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.UUID{}, ErrCustomerNotFound
		}
		return uuid.UUID{}, fmt.Errorf("fetch customer account: %w", err)
	}

	return accountID, nil
}

func fetchAccountBalance(ctx context.Context, rower queryRower, accountID uuid.UUID) (decimal.Decimal, error) {
	const query = `SELECT COALESCE(SUM(amount)::text, '0.0000') FROM ledger_entries WHERE account_id = $1`

	var balanceText string
	if err := rower.QueryRow(ctx, query, accountID).Scan(&balanceText); err != nil {
		return decimal.Decimal{}, err
	}

	balance, err := decimal.NewFromString(balanceText)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("parse account balance: %w", err)
	}

	return normalizeAmount(balance), nil
}

func normalizeAmount(amount decimal.Decimal) decimal.Decimal {
	return amount.RoundBank(4)
}

func ExtractGooseUpStatements(contents string) string {
	upMarker := "-- +goose Up"
	downMarker := "-- +goose Down"

	upIdx := strings.Index(contents, upMarker)
	if upIdx == -1 {
		return ""
	}

	upSection := contents[upIdx+len(upMarker):]
	downIdx := strings.Index(upSection, downMarker)
	if downIdx == -1 {
		return strings.TrimSpace(upSection)
	}

	return strings.TrimSpace(upSection[:downIdx])
}
