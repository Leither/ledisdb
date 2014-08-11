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
	return ctx
}

func (ctx *clientContext) beginTransaction(tx *ledis.Tx) error {
	if ctx.txCtx != nil {
		return errTxDuplication
	}

	ctx.txCtx = newTransactionContext(ctx.app, tx)
	ctx.db = tx.DB
	return nil
}

func (ctx *clientContext) endTransaction() error {
	if ctx.txCtx == nil {
		return errTxMiss
	}

	ctx.db, _ = ctx.app.ldb.Select(ctx.db.Index())
	ctx.txCtx.release()
	ctx.txCtx = nil

	return nil
}

func (ctx *clientContext) acquireTx() *ledis.Tx {
	if ctx.txCtx != nil {
		return ctx.txCtx.tx
	}
	return nil
}

func (ctx *clientContext) release() {
	if ctx.txCtx != nil {
		ctx.txCtx.release()
	}
}
