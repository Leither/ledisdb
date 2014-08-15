package server

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

//dump format
// fileIndex(bigendian int64)|filePos(bigendian int64)
// |keylen(bigendian int32)|key|valuelen(bigendian int32)|value......
//
//key and value are both compressed for fast transfer dump on network using snappy

type BinLogHeader struct {
	AofFileIndex  int64
	AofFileOffset int64
}

func (h *BinLogHeader) WriteTo(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, h.AofFileIndex); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, h.AofFileOffset); err != nil {
		return err
	}
	return nil
}

func (h *BinLogHeader) ReadFrom(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &h.AofFileIndex)
	if err != nil {
		return err
	}

	err = binary.Read(r, binary.BigEndian, &h.AofFileOffset)
	if err != nil {
		return err
	}

	return nil
}

func (app *App) DumpFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return app.dumpTo(f)
}

func (app *App) dumpTo(w io.Writer) error {
	var err error

	app.Lock()
	defer app.Unlock()

	wb := bufio.NewWriterSize(w, 4096)

	//	dump header
	var h *BinLogHeader = new(BinLogHeader)
	if app.aof != nil {
		h.AofFileIndex = int64(app.aof.LogFileIndex())
		h.AofFileOffset = app.aof.LogFilePos()
	}
	if err = h.WriteTo(wb); err != nil {
		return err
	}

	//	dump data
	if err = app.ldb.DumpTo(wb); err != nil {
		return err
	}

	if err = wb.Flush(); err != nil {
		return err
	}

	return nil
}

func (app *App) LoadDumpFile(path string) (*BinLogHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return app.loadDump(f)
}

func (app *App) loadDump(r io.Reader) (*BinLogHeader, error) {
	app.Lock()
	defer app.Unlock()

	rb := bufio.NewReaderSize(r, 4096)

	h := new(BinLogHeader)
	if err := h.ReadFrom(rb); err != nil {
		return nil, err
	}

	if err := app.ldb.LoadDumpFrom(rb); err != nil {
		return nil, err
	}

	return h, nil
}
