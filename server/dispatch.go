package server

import (
	"fmt"
	"github.com/siddontang/ledisdb/ledis"
	"strings"
)

type dispatch struct {
	app       *App
	whandlers []*asyncRequestHandler
}

func newDispatcher(app *App) {
	disp := new(dispatch)
	disp.app = app
	disp.initWHandlers()
	return disp
}

func (disp *dispatch) initWHandlers() {
	var cnt int = int(ledis.MaxDBNumber)

	disp.whandlers = make([]*asyncRequestHandler, 0, cnt)

	for dbNO := 0; dbNO < cnt; dbNO++ {
		hdl := newAsyncRequestHandler(app)
		disp.whandlers = append(disp.whandlers, hdl)
	}
}

func (disp *dispatch) postClientRequest(c client, req *requestContext) {

	txCtx := req.cliCtx.txCtx
	if txCtx != nil {
		txCtx.hdl.handle(req)
		return
	}

	if req.cmd == "quit" {
		req.resp.writeStatus(OK)
		req.resp.flush()
		c.close()
		return
	}

	if isWCommand(req.cmd) {
		hdl := disp.whandlers[req.db.Index()]
		hdl.handle(req)

	} else {
		cliCtx.hdl.handle(req)
	}

	return
}
