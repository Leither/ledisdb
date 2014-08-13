package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/ledisdb/config"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

/*
index file format:
	ledis-aof.00001
	ledis-aof.00002
	ledis-aof.00003

log file format:
	timestamp(bigendian uint32, seconds)|PayloadLen(bigendian uint32)|PayloadData

*/

type Aof struct {
	cfg *config.AofConfig

	path string

	f     *os.File
	wb    *bufio.Writer
	fsize int64

	indexName   string
	fnames      []string
	latestIndex int
}

func NewAof(cfg *config.Config) (*Aof, error) {
	aof := new(Aof)

	aof.cfg = &cfg.AOF
	aof.cfg.Adjust()

	aof.fnames = make([]string, 0, 16)

	aof.path = path.Join(cfg.DataDir, "aof")
	if err := os.MkdirAll(aof.path, os.ModePerm); err != nil {
		return nil, err
	}

	if err := aof.loadIndex(); err != nil {
		return nil, err
	}

	return aof, nil
}

func (aof *Aof) flushIndex() error {
	bakName := fmt.Sprintf("%s.bak", aof.indexName)
	f, err := os.OpenFile(bakName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Error("create aof bak index error %s", err.Error())
		return err
	}

	data := strings.Join(aof.fnames, "\n")

	if _, err := f.WriteString(data); err != nil {
		log.Error("write aof index error %s", err.Error())
		f.Close()
		return err
	}
	f.Close()

	if err := os.Rename(bakName, aof.indexName); err != nil {
		log.Error("rename aof bak index error %s", err.Error())
		return err
	}

	return nil
}

func (aof *Aof) loadIndex() error {
	aof.indexName = path.Join(aof.path, fmt.Sprintf("ledis-aof.index"))
	if _, err := os.Stat(aof.indexName); os.IsNotExist(err) {
		//no index file, nothing to do
	} else {
		indexData, err := ioutil.ReadFile(aof.indexName)
		if err != nil {
			return err
		}

		lines := strings.Split(string(indexData), "\n")
		for _, data := range lines {
			data = strings.Trim(data, "\r\n ")
			if len(data) == 0 {
				continue
			}

			if _, err := os.Stat(path.Join(aof.path, data)); err != nil {
				log.Error("load index line %s error %s", data, err.Error())
				return err
			} else {
				aof.fnames = append(aof.fnames, data)
			}
		}
	}

	var fileNum int
	var err error

	fileNum, err = aof.arrangeFiles()
	if err != nil {
		return err
	}

	if fileNum == 0 {
		aof.latestIndex = 1
	} else {
		lastFname := aof.fnames[fileNum-1]
		fileExt := path.Ext(lastFname)[1:]

		var lastIdx int64
		if lastIdx, err = strconv.ParseInt(fileExt, 10, 32); err != nil {
			log.Error("invalid aof file name %s", err.Error())
			return err
		}
		aof.latestIndex = int(lastIdx)

		// like mysql, if server restart, a new aof will create
		aof.latestIndex++
	}

	return nil
}

func (aof *Aof) latestFileName() string {
	return aof.FormatFileName(aof.latestIndex)
}

func (aof *Aof) openNewFile() error {
	var err error

	fname := aof.latestFileName()
	fpath := path.Join(aof.path, fname)

	if aof.f, err = os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0666); err != nil {
		log.Error("open new aof file error %s", err.Error())
		return err
	}

	if err = aof.register(fname); err != nil {
		return err
	}

	if aof.wb == nil {
		aof.wb = bufio.NewWriterSize(aof.f, 1024)
	} else {
		aof.wb.Reset(aof.f)
	}

	if st, err := aof.f.Stat(); err != nil {
		return err
	} else {
		aof.fsize = st.Size()
	}

	return nil
}

func (aof *Aof) register(fname string) error {
	aof.fnames = append(aof.fnames, fname)
	if _, err := aof.arrangeFiles(); err != nil {
		return err
	}
	return aof.flushIndex()
}

func (aof *Aof) arrangeFiles() (fnum int, err error) {
	fnum = len(aof.fnames)
	if aof.cfg.MaxFileNum > 0 && fnum > aof.cfg.MaxFileNum {
		aof.purgeFiles(fnum - aof.cfg.MaxFileNum)

		fnum = len(aof.fnames)
		err = aof.flushIndex()
	}
	return
}

func (aof *Aof) purgeFiles(n int) {
	for i := 0; i < n; i++ {
		logPath := path.Join(aof.path, aof.fnames[i])
		os.Remove(logPath)
	}

	copy(aof.fnames[0:], aof.fnames[n:])
	aof.fnames = aof.fnames[0 : len(aof.fnames)-n]
}

func (aof *Aof) checkLogFileSize() bool {
	if aof.f != nil && aof.fsize >= int64(aof.cfg.MaxFileSize) {
		aof.latestIndex++

		aof.f.Close()
		aof.f = nil
		aof.fsize = 0

		return true
	}

	return false
}

func (aof *Aof) Close() {
	if aof.f != nil {
		aof.f.Close()
		aof.f = nil
		aof.fsize = 0
	}
}

func (aof *Aof) LogNames() []string {
	return aof.fnames
}

func (aof *Aof) LogFileName() string {
	return aof.latestFileName()
}

func (aof *Aof) LogFilePos() int64 {
	if aof.f == nil {
		return 0
	} else {
		st, _ := aof.f.Stat()
		return st.Size()
	}
}

func (aof *Aof) FileIndex() int {
	return aof.latestIndex
}

func (aof *Aof) FormatFileName(index int) string {
	return fmt.Sprintf("ledis-aof.%07d", index)
}

func (aof *Aof) FormatFilePath(index int) string {
	return path.Join(aof.path, aof.FormatFileName(index))
}

func (aof *Aof) Path() string {
	return aof.path
}

func (aof *Aof) Log(args ...[]byte) error {
	var err error

	if aof.f == nil {
		if err = aof.openNewFile(); err != nil {
			return err
		}
	}

	//we treat log many args as a batch, so use same createTime
	var appendSize uint32 = 0
	var createTime uint32 = uint32(time.Now().Unix())

	for _, data := range args {
		payLoadLen := uint32(len(data))

		if err := binary.Write(aof.wb, binary.BigEndian, createTime); err != nil {
			return err
		}

		if err := binary.Write(aof.wb, binary.BigEndian, payLoadLen); err != nil {
			return err
		}

		if _, err := aof.wb.Write(data); err != nil {
			return err
		}

		appendSize += (8 + payLoadLen)
	}

	if err = aof.wb.Flush(); err != nil {
		log.Error("write log error %s", err.Error())
		return err
	}

	aof.fsize += int64(appendSize)
	aof.checkLogFileSize()

	return nil
}
