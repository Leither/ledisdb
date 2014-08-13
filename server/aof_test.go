package server

import (
	"github.com/siddontang/ledisdb/config"
	"io/ioutil"
	"os"
	"testing"
)

func TestAof(t *testing.T) {
	cfg := new(config.Config)

	cfg.AOF.MaxFileNum = 2
	cfg.AOF.MaxFileSize = 1024
	cfg.DataDir = "/tmp/ledis_aof"

	os.RemoveAll(cfg.DataDir)

	aof, err := NewAof(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var round = 3
	for i := 0; i < round; i++ {
		if err := aof.Log(make([]byte, cfg.AOF.MaxFileSize)); err != nil {
			t.Fatal(err)
		}
	}

	if fs, err := ioutil.ReadDir(aof.Path()); err != nil {
		t.Fatal(err)
	} else if len(fs) != round {
		//	ps : including the index file
		t.Fatal(len(fs))
	}

	if idx := aof.FileIndex(); idx != round+1 {
		t.Fatal(idx)
	}

	logFname1 := aof.LogFileName()

	//	2nd open
	aof, err = NewAof(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if fs, err := ioutil.ReadDir(aof.Path()); err != nil {
		t.Fatal(err)
	} else if len(fs) != round {
		//	ps : including the index file
		t.Fatal(len(fs))
	}

	if idx := aof.FileIndex(); idx != round+1 {
		t.Fatal(idx)
	}

	if logFname2 := aof.LogFileName(); logFname1 != logFname2 {
		t.Fatal(logFname1)
	}
}
