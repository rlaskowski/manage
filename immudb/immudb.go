package immudb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
)

type ImmuDBRows struct {
	rowr    sql.RowReader
	row     sql.Row
	rwmutex sync.RWMutex
}

func (i *ImmuDBRows) Next() bool {
	i.rwmutex.Lock()
	defer i.rwmutex.Unlock()

	r, err := i.rowr.Read()

	if err != nil {
		return false
	}

	i.row = *r

	return true
}

func (i *ImmuDBRows) Columns() ([]sql.ColDescriptor, error) {
	return i.rowr.Columns()
}

func (i *ImmuDBRows) Scan(params ...interface{}) error {
	if len(i.row.ValuesBySelector) != len(params) {
		return fmt.Errorf("different number of columns in row, expected %d got %d", len(params), len(i.row.ValuesBySelector))
	}

	cols, err := i.rowr.Columns()
	if err != nil {
		return fmt.Errorf("bad columns definition due to: %s", err.Error())
	}

	i.rwmutex.Lock()
	defer i.rwmutex.Unlock()

	index := 0
	for _, c := range cols {
		key := fmt.Sprintf("(%s.%s.%s)", c.Database, c.Table, c.Column)

		valt, ok := i.row.ValuesBySelector[key]

		if ok {
			if err := i.parseType(params[index], valt.Value()); err != nil {
				return err
			}
		}

		index++
	}

	return nil
}

func (im *ImmuDBRows) parseType(dst, src interface{}) error {
	switch s := src.(type) {
	case string:
		switch d := dst.(type) {
		case *string:
			*d = s
			return nil
		}
	case time.Time:
		switch d := dst.(type) {
		case *time.Time:
			*d = s
			return nil
		case *string:
			*d = s.Format(time.RFC3339Nano)
			return nil
		case *[]byte:
			*d = []byte(s.Format(time.RFC3339Nano))
			return nil
		}
	}

	return nil
}

type ImmuDB struct {
	engine *sql.Engine
}

func NewImmuDB(engine *sql.Engine) *ImmuDB {
	return &ImmuDB{engine}
}

// Starts a transaction
func (im *ImmuDB) Begin() (*Tx, error) {
	return im.BeginTx(context.Background())
}

// Starts a transaction
func (im *ImmuDB) BeginTx(ctx context.Context) (*Tx, error) {
	tx, _, err := im.engine.Exec("BEGIN TRANSACTION;", nil, nil)
	if err != nil {
		return nil, err
	}

	return &Tx{
		immudb: im,
		tx:     tx,
		ctx:    ctx,
	}, nil
}

// Selecting data by params
func (im *ImmuDB) Query(sql string, params map[string]interface{}) (*ImmuDBRows, error) {
	return im.QueryContext(context.Background(), sql, params)
}

// Selecting data by params with context
func (im *ImmuDB) QueryContext(ctx context.Context, sql string, params map[string]interface{}) (*ImmuDBRows, error) {
	return im.query(ctx, sql, params, nil)
}

func (im *ImmuDB) query(ctx context.Context, sql string, params map[string]interface{}, tx *sql.SQLTx) (*ImmuDBRows, error) {
	rowr, err := im.engine.Query(sql, params, tx)

	if err != nil {
		return nil, err
	}

	rows := &ImmuDBRows{
		rowr: rowr,
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return rows, nil
}

type Tx struct {
	immudb *ImmuDB
	tx     *sql.SQLTx
	ctx    context.Context
}

func (tx *Tx) Exec(sql string, params map[string]interface{}) error {
	return tx.ExecContext(context.Background(), sql, params)
}

func (tx *Tx) ExecContext(ctx context.Context, sql string, params map[string]interface{}) error {
	_, _, err := tx.immudb.engine.Exec(sql, params, tx.tx)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil
}

func (tx *Tx) Query(sql string, params map[string]interface{}) (*ImmuDBRows, error) {
	return tx.QueryContext(context.Background(), sql, params)
}

func (tx *Tx) QueryContext(ctx context.Context, sql string, params map[string]interface{}) (*ImmuDBRows, error) {
	return tx.immudb.query(ctx, sql, params, tx.tx)
}

func (tx *Tx) Commit() error {
	_, _, err := tx.immudb.engine.Exec("COMMIT;", nil, tx.tx)
	if err != nil {
		return err
	}

	return nil
}

func (tx *Tx) Rollback() error {
	_, _, err := tx.immudb.engine.Exec("ROLLBACK;", nil, tx.tx)
	if err != nil {
		return err
	}

	return nil
}
