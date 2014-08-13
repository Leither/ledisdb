package ledis

import (
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/store"
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
