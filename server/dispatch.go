package server

import (
	"github.com/siddontang/ledisdb/ledis"
)

type dispatch struct {
	app       *App
	whandlers []*asyncRequestHandler
}

func newDispatcher(app *App) *dispatch {
	disp := new(dispatch)
	disp.app = app

	disp.initWHandlers(app)

	return disp
}

func (disp *dispatch) initWHandlers(app *App) {
	var cnt int = int(ledis.MaxDBNumber)

	whandlers := make([]*asyncRequestHandler, 0, cnt)
	for dbNO := 0; dbNO < cnt; dbNO++ {
		hdl := newAsyncRequestHandler(app)
		whandlers = append(whandlers, hdl)
	}
	disp.whandlers = whandlers
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
		req.cliCtx.hdl.handle(req)
	}

	return
}
