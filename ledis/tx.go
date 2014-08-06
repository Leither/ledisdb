package ledis

import (
	"errors"
	"github.com/siddontang/ledisdb/store"
	"sync"
)

var (
	ErrNestTx = errors.New("nest transaction not supported")
	ErrTxDone = errors.New("Transaction has already been committed or rolled back")
)

type batch struct {
	store.WriteBatch

	sync.Locker
}

type dbBatchLocker struct {
	sync.Mutex
	dbLock *sync.RWMutex
}

type dbTxLocker struct {
}

func (l *dbTxLocker) Lock() {
}

func (l *dbTxLocker) Unlock() {
}

func (l *dbBatchLocker) Lock() {
	l.dbLock.RLock()
	l.Mutex.Lock()
}

func (l *dbBatchLocker) Unlock() {
	l.Mutex.Unlock()
	l.dbLock.RUnlock()
}

func (db *DB) newBatch() *batch {
	b := new(batch)

	b.WriteBatch = db.bucket.NewWriteBatch()
	b.Locker = &dbBatchLocker{dbLock: db.dbLock}

	return b
}

func (b *batch) Lock() {
	b.Locker.Lock()
}

func (b *batch) Unlock() {
	b.Rollback()
	b.Locker.Unlock()
}

type Tx struct {
	*DB

	m *sync.RWMutex

	tx *store.Tx
}

func (db *DB) IsInTransaction() bool {
	return db.isTx
}

// Begin a transaction, it will block all other write operations before calling Commit or Rollback.
// You must be very careful to prevent long-time transaction.
func (db *DB) Begin() (*Tx, error) {
	if db.isTx {
		return nil, ErrNestTx
	}

	tx := new(Tx)
	tx.m = db.dbLock

	tx.m.Lock()

	d := new(DB)

	d.l = db.l

	d.sdb = db.sdb

	var err error
	tx.tx, err = db.sdb.Begin()
	if err != nil {
		return nil, err
	}

	d.bucket = tx.tx

	d.isTx = true

	d.index = db.index

	b := new(batch)
	b.WriteBatch = d.bucket.NewWriteBatch()
	b.Locker = &dbTxLocker{}

	d.kvBatch = b
	d.listBatch = b
	d.hashBatch = b
	d.zsetBatch = b
	d.binBatch = b

	tx.DB = d
	return tx, nil
}

func (tx *Tx) Commit() error {
	if tx.tx == nil {
		return ErrTxDone
	}

	err := tx.tx.Commit()
	tx.tx = nil

	tx.m.Unlock()
	tx.DB = nil
	return err
}

func (tx *Tx) Rollback() error {
	if tx.tx == nil {
		return ErrTxDone
	}

	err := tx.tx.Rollback()
	tx.tx = nil

	tx.m.Unlock()
	tx.DB = nil
	return err
}
