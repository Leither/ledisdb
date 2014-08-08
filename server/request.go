package server

import (
	"bytes"
	"github.com/siddontang/ledisdb/ledis"
	"io"
	"time"
)

type responseWriter interface {
	writeError(error)
	writeStatus(string)
	writeInteger(int64)
	writeBulk([]byte)
	writeArray([]interface{})
	writeSliceArray([][]byte)
	writeFVPairArray([]ledis.FVPair)
	writeScorePairArray([]ledis.ScorePair, bool)
	writeBulkFrom(int64, io.Reader)
	flush()
}

type requestContext struct {
	app *App
	ldb *ledis.Ledis
	db  *ledis.DB

	tx *ledis.Tx

	remoteAddr string
	cmd        string
	args       [][]byte

	syncBuf     bytes.Buffer
	compressBuf []byte

	reqErr chan error

	cliCtx *clientContext
	appCtx *appContext

	resp responseWriter
}

func newRequestContext(app *App) *requestContext {
	req := new(requestContext)

	req.app = app
	req.ldb = app.ldb
	req.db, _ = app.ldb.Select(0) //use default db

	req.compressBuf = make([]byte, 256)
	req.reqErr = make(chan error)

	return req
}

// type irequestHandler interface {
// 	handle(*requestContext)
// }

type requestHandler struct {
	app *App
	buf bytes.Buffer
	res chan error
}

type asyncRequestHandler struct {
	requestHandler
	reqs chan *requestContext
}

func newReuqestHandler(app *App) *requestHandler {
	hdl := new(requestHandler)
	hdl.app = app
	hdl.res = make(chan error)

	return hdl
}

func newAsyncRequestHandler(app *App) *asyncRequestHandler {
	hdl := new(asyncRequestHandler)

	hdl.requestHandler = newReuqestHandler(app) // ??????????????????

	hdl.reqs = make(chan *requestContext, 128)

	go hdl.run()
	return hdl
}

func (h *asyncRequestHandler) run() {
	var req *requestContext
	var end bool = false
	for !end {
		req <- h.reqs
		if req != nil {
			h.requestHandler.handle(req)
			req.reqErr = <-nil // todo ........................
		}
	}
}

func (h *asyncRequestHandler) handle(req *requestContext) {
	h.reqs <- req
	res := <-req.reqErr
}

func (h *requestHandler) handle(req *requestContext) {
	var err error

	start := time.Now()

	if len(req.cmd) == 0 {
		err = ErrEmptyCommand
	} else if exeCmd, ok := regCmds[req.cmd]; !ok {
		err = ErrNotFound
	} else {
		req.db = req.cliCtx.db

		go func() {
			h.res <- exeCmd(req)
		}()
		err = <-h.res
	}

	duration := time.Since(start)

	if hdl.app.access != nil {
		fullCmd := hdl.catGenericCommand()
		cost := duration.Nanoseconds() / 1000000

		truncateLen := len(fullCmd)
		if truncateLen > 256 {
			truncateLen = 256
		}

		hdl.app.access.Log(req.remoteAddr, cost, fullCmd[:truncateLen], err)
	}

	if err != nil {
		req.resp.writeError(err)
	}
	req.resp.flush()
	return
}

// func (h *requestHandler) catFullCommand(req *requestContext) []byte {
//
// 	// if strings.HasSuffix(cmd, "expire") {
// 	// 	catExpireCommand(c, buffer)
// 	// } else {
// 	// 	catGenericCommand(c, buffer)
// 	// }
//
// 	return h.catGenericCommand(req)
// }

func (h *requestHandler) catGenericCommand() []byte {
	buffer := h.buf
	buffer.Reset()

	buffer.Write([]byte(req.cmd))

	for _, arg := range req.args {
		buffer.WriteByte(' ')
		buffer.Write(arg)
	}

	return buffer.Bytes()
}
