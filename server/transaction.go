package server

import (
	"error"
	"github.com/siddontang/ledisdb/ledis"
	"strings"
)

var txUnsopportedCommands = make(map[string]byte)

var errTxMiss = errors.New("transaction context miss")
var errTxDuplication = errors.New("duplicate transaction")
var errTxInvalidOperation = errors.New("invalid operation in transaction")

type transactionContext struct {
	tx  *ledis.Tx
	hdl *transactionHandler
}

type transactionHandler struct {
	worker *requestHandler
}

func newTransactionContext(app *App, tx *ledis.Tx) *transactionContext {
	ctx := new(transactionContext)
	ctx.tx = tx
	ctx.hdl = newTransactionHandler(app)
	return ctx
}

func (ctx *transactionContext) release() {
	//	todo ......
}

func newTransactionHandler(app *App) *transactionHandler {
	hdl := new(transactionHandler)
	hdl.worker = newReuqestHandler(app)
	return hdl
}

func (hdl *transactionHandler) handle(req *requestContext) error {
	if ok, _ := txUnsopportedCommands[req.cmd]; ok {
		return errTxInvalidOperation
	}

	return hdl.worker.handle(req)
}

func txReject(name string) {
	txUnsopportedCommands[strings.ToLower(name)] = 1
}

func init() {
	// todo ... like write commands , config in universe ?

	txReject("select")
	txReject("echo")
	txReject("ping") // !!!!!!!!!!!!!

	txReject("slaveof")
	txReject("fullsync")
	txReject("sync")

	txReject("quit")
}
