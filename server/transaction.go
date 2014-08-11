package server

import (
	"errors"
	"github.com/siddontang/ledisdb/ledis"
	"strings"
)

var txUnsopportedCommands = map[string]byte{}

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
	if _, ok := txUnsopportedCommands[req.cmd]; ok {
		return errTxInvalidOperation
	}

	hdl.worker.handle(req)
	return nil
}

func txReject(name string) {
	txUnsopportedCommands[strings.ToLower(name)] = 1
}

func init() {
	txReject("select")
	txReject("echo")
	txReject("ping") // !!!!!!!!!!!!!
	txReject("slaveof")
	txReject("fullsync")
	txReject("sync")
	txReject("quit")
}
