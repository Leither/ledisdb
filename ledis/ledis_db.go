package ledis

import (
	"github.com/siddontang/ledisdb/store"
	"sync"
)

type ibucket interface {
	Get(key []byte) ([]byte, error)

	Put(key []byte, value []byte) error
	Delete(key []byte) error

	NewIterator() *store.Iterator

	NewWriteBatch() store.WriteBatch

	RangeIterator(min []byte, max []byte, rangeType uint8) *store.RangeLimitIterator
	RevRangeIterator(min []byte, max []byte, rangeType uint8) *store.RangeLimitIterator
	RangeLimitIterator(min []byte, max []byte, rangeType uint8, offset int, count int) *store.RangeLimitIterator
	RevRangeLimitIterator(min []byte, max []byte, rangeType uint8, offset int, count int) *store.RangeLimitIterator
}

type DB struct {
	l *Ledis

	sdb *store.DB

	bucket ibucket

	dbLock sync.RWMutex

	index uint8

	kvBatch   *batch
	listBatch *batch
	hashBatch *batch
	zsetBatch *batch
	binBatch  *batch

	isTx bool
}

func (l *Ledis) newDB(index uint8) *DB {
	d := new(DB)

	d.l = l

	d.sdb = l.ldb

	d.bucket = d.sdb

	d.isTx = false

	d.index = index

	d.kvBatch = d.newBatch()
	d.listBatch = d.newBatch()
	d.hashBatch = d.newBatch()
	d.zsetBatch = d.newBatch()
	d.binBatch = d.newBatch()

	return d
}

func (db *DB) FlushAll() (drop int64, err error) {
	all := [...](func() (int64, error)){
		db.flush,
		db.lFlush,
		db.hFlush,
		db.zFlush,
		db.bFlush}

	for _, flush := range all {
		if n, e := flush(); e != nil {
			err = e
			return
		} else {
			drop += n
		}
	}

	return
}

func (db *DB) newEliminator() *elimination {
	eliminator := newEliminator(db)
	eliminator.regRetireContext(KVType, db.kvBatch, db.delete)
	eliminator.regRetireContext(ListType, db.listBatch, db.lDelete)
	eliminator.regRetireContext(HashType, db.hashBatch, db.hDelete)
	eliminator.regRetireContext(ZSetType, db.zsetBatch, db.zDelete)
	eliminator.regRetireContext(BitType, db.binBatch, db.bDelete)

	return eliminator
}

func (db *DB) flushRegion(t *batch, minKey []byte, maxKey []byte) (drop int64, err error) {
	it := db.bucket.RangeIterator(minKey, maxKey, store.RangeROpen)
	for ; it.Valid(); it.Next() {
		t.Delete(it.RawKey())
		drop++
		if drop&1023 == 0 {
			if err = t.Commit(); err != nil {
				return
			}
		}
	}
	it.Close()
	return
}
