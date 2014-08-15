package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/ledisdb/config"
	"io"
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

var (
	ErrSkipRecord = errors.New("skip to next record")
)

var (
	errInvalidAofRecord = errors.New("invalid aof record")
)

type placeRecord func(uint32, *aofRecord) error

type aofRecord struct {
	db      int
	fullcmd []byte
}

func (rcd *aofRecord) extract() []byte {
	cmdLen := 0
	if rcd.fullcmd == nil {
		cmdLen = len(rcd.fullcmd)
	}

	if cmdLen == 0 {
		return nil
	}

	sz := make([]byte, 3+cmdLen)
	sz[0] = uint8(rcd.db)
	copy(sz[1:], rcd.fullcmd)

	return sz
}

func (rcd *aofRecord) setup(data []byte) error {
	if len(data) < 3 {
		return errInvalidAofRecord
	}

	rcd.db = int(data[0])
	rcd.fullcmd = data[1:]
	return nil
}

type aofAnchor struct {
	fileIndex  int64
	fileOffset int64
}

type Aof struct {
	cfg *config.AofConfig

	path string

	f  *os.File
	wb *bufio.Writer

	fnames    []string
	indexName string
	index     int64
	fsize     int64
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

	//	like mysql, if server restart, create a new file
	aof.openNewFile()

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

	aof.index = int64(0)
	if fileNum != 0 {
		lastFname := aof.fnames[fileNum-1]
		fileExt := path.Ext(lastFname)[1:]

		var lastIdx int64
		if lastIdx, err = strconv.ParseInt(fileExt, 10, 32); err != nil {
			log.Error("invalid aof file name %s", err.Error())
			return err
		}
		aof.index = lastIdx
	}

	return nil
}

func (aof *Aof) openNewFile() error {
	var err error

	aof.index++
	aof.fsize = 0

	fname := aof.FormatFileName(aof.index)
	fpath := path.Join(aof.path, fname)

	if aof.f, err = os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0666); err != nil {
		log.Error("open new aof file error %s", err.Error())
		return err
	}

	st, _ := aof.f.Stat()
	aof.fsize = st.Size()

	if aof.wb == nil {
		aof.wb = bufio.NewWriterSize(aof.f, 1024)
	} else {
		aof.wb.Reset(aof.f)
	}

	if err = aof.register(fname); err != nil {
		return err
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
	return aof.FormatFileName(aof.index)
}

func (aof *Aof) LogFilePos() int64 {
	if aof.f == nil {
		return 0
	} else {
		st, _ := aof.f.Stat()
		return st.Size()
	}
}

func (aof *Aof) LogFileIndex() int64 {
	return aof.index
}

func (aof *Aof) FormatFileName(index int64) string {
	return fmt.Sprintf("ledis-aof.%07d", index)
}

func (aof *Aof) FormatFilePath(index int64) string {
	return path.Join(aof.path, aof.FormatFileName(index))
}

func (aof *Aof) Path() string {
	return aof.path
}

func (aof *Aof) Log(args ...[]byte) error {
	var err error

	if aof.f == nil {
		if err := aof.openNewFile(); err != nil {
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

func (aof *Aof) ReadN(anchor *aofAnchor, place placeRecord, limit int) error {
	filePath := aof.FormatFilePath(anchor.fileIndex)
	f, err := os.Open(filePath) // todo ... optimize ?

	if err != nil {
		if os.IsNotExist(err) {
			anchor.fileIndex = -1
			anchor.fileOffset = 0
			err = nil
		}
		return err
	}

	var readed uint32
	readed, err = AofReadN(f, place, limit)

	if err != nil && err == io.EOF {
		if anchor.fileIndex < aof.index {
			anchor.fileIndex++
			anchor.fileOffset = 0
		}
	}

	anchor.fileOffset += int64(readed)

	f.Close()

	return nil
}

func (aof *Aof) CopyN(anchor *aofAnchor, w io.Writer, limit int) error {
	filePath := aof.FormatFilePath(anchor.fileIndex)
	f, err := os.Open(filePath) // todo ... optimize ?

	if err != nil {
		if os.IsNotExist(err) {
			anchor.fileIndex = -1
			anchor.fileOffset = 0
			err = nil
		}
		return err
	}

	var size uint32
	size, err = AofCopyN(f, w, limit)

	if err != nil && err == io.EOF {
		if anchor.fileIndex < aof.index {
			anchor.fileIndex++
			anchor.fileOffset = 0
		}
	}

	anchor.fileOffset += int64(size)

	f.Close()

	return nil
}

func AofReadN(r io.Reader, place placeRecord, limit int) (uint32, error) {
	var createTime uint32
	var dataLen uint32
	var dataBuf bytes.Buffer
	var err error

	var readed uint32

	if limit < 0 {
		limit = 0xFFFFFFF
	}

	aofRcd := new(aofRecord)

	for i := 0; i < limit; i++ {
		if err = binary.Read(r, binary.BigEndian, &createTime); err != nil {
			return readed, err
		}

		if err = binary.Read(r, binary.BigEndian, &dataLen); err != nil {
			return readed, err
		}

		if _, err = io.CopyN(&dataBuf, r, int64(dataLen)); err != nil {
			return readed, err
		}

		if err = aofRcd.setup(dataBuf.Bytes()); err != nil {
			return readed, ErrSkipRecord
		}

		err = place(createTime, aofRcd)
		if err != nil && err != ErrSkipRecord {
			return readed, err
		}

		readed += (8 + dataLen)

		dataBuf.Reset()
	}

	return readed, nil
}

func AofCopyN(r io.Reader, w io.Writer, limit int) (uint32, error) {
	var copySize uint32
	var dataLen uint32
	var err error

	if limit < 0 {
		limit = 0xFFFFFFF
	}

	var ctimeLen int64 = 4

	for i := 0; i < limit; i++ {
		//	create time
		if _, err = io.CopyN(w, r, ctimeLen); err != nil {
			return copySize, err
		}

		//	data
		if err = binary.Read(r, binary.BigEndian, &dataLen); err != nil {
			return copySize, err
		}

		if _, err = io.CopyN(w, r, int64(dataLen+4)); err != nil {
			return copySize, err
		}

		copySize += (8 + dataLen)
	}

	return copySize, nil
}
