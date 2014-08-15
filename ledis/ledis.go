package ledis

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-snappy/snappy"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/store"
	"io"
	"sync"
	"time"
)

type Ledis struct {
	sync.Mutex

	cfg *config.Config

	ldb *store.DB
	dbs [MaxDBNumber]*DB

	binlog *BinLog

	quit chan struct{}
	jobs *sync.WaitGroup
}

func Open(cfg *config.Config) (*Ledis, error) {
	if len(cfg.DataDir) == 0 {
		fmt.Printf("no datadir set, use default %s\n", config.DefaultDataDir)
		cfg.DataDir = config.DefaultDataDir
	}

	ldb, err := store.Open(cfg)
	if err != nil {
		return nil, err
	}

	l := new(Ledis)

	l.quit = make(chan struct{})
	l.jobs = new(sync.WaitGroup)

	l.ldb = ldb

	// if cfg.BinLog.MaxFileNum > 0 && cfg.BinLog.MaxFileSize > 0 {
	// 	println("binlog will be refactored later, use your own risk!!!")
	// 	l.binlog, err = NewBinLog(cfg)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// } else {
	// 	l.binlog = nil
	// }

	for i := uint8(0); i < MaxDBNumber; i++ {
		l.dbs[i] = l.newDB(i)
	}

	l.activeExpireCycle()

	return l, nil
}

func (l *Ledis) Close() {
	close(l.quit)
	l.jobs.Wait()

	l.ldb.Close()

	// if l.binlog != nil {
	// 	l.binlog.Close()
	// 	l.binlog = nil
	// }
}

func (l *Ledis) Select(index int) (*DB, error) {
	if index < 0 || index >= int(MaxDBNumber) {
		return nil, fmt.Errorf("invalid db index %d", index)
	}

	return l.dbs[index], nil
}

func (l *Ledis) FlushAll() error {
	for index, db := range l.dbs {
		if _, err := db.FlushAll(); err != nil {
			log.Error("flush db %d error %s", index, err.Error())
		}
	}

	return nil
}

// very dangerous to use
func (l *Ledis) DataDB() *store.DB {
	return l.ldb
}

func (l *Ledis) activeExpireCycle() {
	var executors []*elimination = make([]*elimination, len(l.dbs))
	for i, db := range l.dbs {
		executors[i] = db.newEliminator()
	}

	l.jobs.Add(1)
	go func() {
		tick := time.NewTicker(1 * time.Second)
		end := false
		done := make(chan struct{})
		for !end {
			select {
			case <-tick.C:
				go func() {
					for _, eli := range executors {
						eli.active()
					}
					done <- struct{}{}
				}()
				<-done
			case <-l.quit:
				end = true
				break
			}
		}

		tick.Stop()
		l.jobs.Done()
	}()
}

// func (l *Ledis) PutRaw(key []byte, val []byte) error {
// 	l.Lock()
// 	err := l.ldb.Put(key, val)
// 	l.Unlock()
// 	return err
// }

// func (l *Ledis) DeleteRaw(key []byte) error {
// 	l.Lock()
// 	err := l.ldb.Delete()
// 	l.Unlock()
// 	return err
// }

func (l *Ledis) DumpTo(wb io.Writer) error {
	var key []byte
	var value []byte
	var err error

	compressBuf := make([]byte, 4096)

	it := l.ldb.NewIterator()
	it.SeekToFirst()

	for ; it.Valid(); it.Next() {
		key = it.Key()
		value = it.Value()

		if key, err = snappy.Encode(compressBuf, key); err != nil {
			return err
		}

		if err = binary.Write(wb, binary.BigEndian, uint16(len(key))); err != nil {
			return err
		}

		if _, err = wb.Write(key); err != nil {
			return err
		}

		if value, err = snappy.Encode(compressBuf, value); err != nil {
			return err
		}

		if err = binary.Write(wb, binary.BigEndian, uint32(len(value))); err != nil {
			return err
		}

		if _, err = wb.Write(value); err != nil {
			return err
		}
	}

	compressBuf = nil
	return nil
}

func (l *Ledis) LoadDumpFrom(rb io.Reader) error {
	var err error

	var keyBuf bytes.Buffer
	var valueBuf bytes.Buffer

	var deKeyBuf []byte = make([]byte, 4096)
	var deValueBuf []byte = make([]byte, 4096)

	var keyLen uint16
	var valueLen uint32
	var key, value []byte

	for {
		if err = binary.Read(rb, binary.BigEndian, &keyLen); err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			break
		}

		if _, err = io.CopyN(&keyBuf, rb, int64(keyLen)); err != nil {
			return err
		}

		if key, err = snappy.Decode(deKeyBuf, keyBuf.Bytes()); err != nil {
			return err
		}

		if err = binary.Read(rb, binary.BigEndian, &valueLen); err != nil {
			return err
		}

		if _, err = io.CopyN(&valueBuf, rb, int64(valueLen)); err != nil {
			return err
		}

		if value, err = snappy.Decode(deValueBuf, valueBuf.Bytes()); err != nil {
			return err
		}

		if err = l.ldb.Put(key, value); err != nil {
			return err
		}

		keyBuf.Reset()
		valueBuf.Reset()
	}

	deKeyBuf = nil
	deValueBuf = nil

	return nil
}
