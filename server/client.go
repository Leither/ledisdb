package server

import (
	"github.com/siddontang/ledisdb/ledis"
)

type client interface {
	close() error
	context() *clientContext
}

type clientContext struct {
	app   *App
	db    *ledis.DB
	txCtx *transactionContext
	hdl   *requestHandler
}

func newClientContext(app *App) *clientContext {
	ctx := new(clientContext)
	ctx.app = app
	ctx.db, _ = app.ldb.Select(0)
	ctx.hdl = newReuqestHandler(app)
	ctx.txCtx = newTransactionContext(ctx.app)

	return ctx
}

func (ctx *clientContext) beginTx(tx *ledis.Tx) error {
	if ctx.txCtx.isProcessing() {
		return errTxDuplication
	}

	ctx.txCtx.bind(tx)
	ctx.db = tx.DB

	return nil
}

func (ctx *clientContext) endTx() error {
	if !ctx.txCtx.isProcessing() {
		return errTxMiss
	}

	ctx.db, _ = ctx.app.ldb.Select(ctx.db.Index())
	ctx.txCtx.reset()

	return nil
}

func (ctx *clientContext) release() {
	ctx.txCtx.reset()
}
