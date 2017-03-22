package sqlite

import (
	"database/sql"
	sql2 "github.com/golang-plus/database/sql"
	"github.com/golang-plus/errors"

	_ "github.com/mattn/go-sqlite3"
)

// Rows represents rows returned from SQL database.
type rows struct {
	*sql.Rows
}

// Next moves the point to next row.
func (r *rows) Next() bool {
	return r.Rows.Next()
}

// Scan parses the data from current row.
func (r *rows) Scan(dest ...interface{}) error {
	err := r.Rows.Scan(dest...)
	if err != nil {
		return errors.Wrap(err, "could not parse columns in current row")
	}

	return nil
}

// Transaction represents a SQL transaction.
type transaction struct {
	*sql.Tx
}

// Execute executes the command and returns the number of rows affected in the transaction.
func (t *transaction) Execute(statement string, args ...interface{}) (int64, error) {
	result, err := t.Tx.Exec(statement, args...)
	if err != nil {
		return 0, errors.Wrapf(err, "could not execute sql statement %q", statement)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "could not get number of rows affected")
	}

	return n, nil
}

// Query returns the rows selected in the transaction.
func (t *transaction) Query(statement string, args ...interface{}) (sql2.Rows, error) {
	rs, err := t.Tx.Query(statement, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "could not query rows %q", statement)
	}

	return &rows{rs}, nil
}

// Rollback aborts the transaction.
func (t *transaction) Rollback() error {
	err := t.Tx.Rollback()
	if err != nil {
		return errors.Wrap(err, "could not abort the transaction")
	}

	return nil
}

// Commit commits the transaction.
func (t *transaction) Commit() error {
	err := t.Tx.Commit()
	if err != nil {
		return errors.Wrap(err, "could not commit the transaction")
	}

	return nil
}

// Database represents a SQL database.
type database struct {
	*sql.DB
}

// Execute executes the command and returns the number of rows affected.
func (d *database) Execute(statement string, args ...interface{}) (int64, error) {
	result, err := d.DB.Exec(statement, args...)
	if err != nil {
		return 0, errors.Wrapf(err, "could not execute sql statement %q", statement)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "could not get number of rows affected")
	}

	return n, nil
}

// Query returns the rows selected.
func (d *database) Query(statement string, args ...interface{}) (sql2.Rows, error) {
	rs, err := d.DB.Query(statement, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "could not query rows %q", statement)
	}

	return &rows{rs}, nil
}

// Begin starts a transaction.
func (d *database) Begin() (sql2.Transaction, error) {
	tx, err := d.DB.Begin()
	if err != nil {
		return nil, errors.Wrap(err, "could not start a transaction")
	}

	return &transaction{tx}, nil
}

// NewDatabase returns a new MySQL Database.
func NewDatabase(dataSource string) (sql2.Database, error) {
	db, err := sql.Open("sqlite3", dataSource)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open the database %q", dataSource)
	}

	return &database{
		DB: db,
	}, nil
}

// New is short to NewDatabase func.
func New(dataSource string) (sql2.Database, error) {
	return NewDatabase(dataSource)
}
