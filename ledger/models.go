package ledger

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type Customer struct {
	ID        uuid.UUID
	AccountID uuid.UUID
	Name      string
	CreatedAt time.Time
}

type Pool struct {
	ID                 uuid.UUID
	AccountID          uuid.UUID
	Name               string
	ContributionAmount decimal.Decimal
	CreatedAt          time.Time
}

type Transaction struct {
	ID          uuid.UUID
	PoolID      *uuid.UUID
	Type        string
	Description string
	CreatedAt   time.Time
}

type LedgerEntry struct {
	ID            uuid.UUID
	TransactionID uuid.UUID
	AccountID     uuid.UUID
	Amount        decimal.Decimal
	CreatedAt     time.Time
}
