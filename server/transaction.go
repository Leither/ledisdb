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
	tx   *ledis.Tx
	hdl  *requestHandler
	acts []txAction
}

type txAction struct {
	cmd  string
	args [][]byte
}

func newTransactionContext(app *App) *transactionContext {
	ctx := new(transactionContext)

	ctx.hdl = newReuqestHandler(app) // off binlog as default
	ctx.acts = []txAction{}

	return ctx
}

func (ctx *transactionContext) bind(tx *ledis.Tx) {
	ctx.tx = tx
}

func (ctx *transactionContext) isProcessing() bool {
	return ctx.tx != nil
}

func (ctx *transactionContext) reset() {
	ctx.acts = ctx.acts[0:0]

	if ctx.tx != nil {
		ctx.tx.Rollback()
		ctx.tx = nil
	}
}

func (ctx *transactionContext) process(req *requestContext) error {
	if _, ok := txUnsopportedCommands[req.cmd]; ok {
		req.resp.writeError(errTxInvalidOperation)
		req.resp.flush()
		return errTxInvalidOperation
	}

	err := ctx.hdl.handle(req)

	if err == nil {
		switch req.cmd {
		case "commit":
			ctx.acts = append(ctx.acts, txAction{"commit", nil})
			// todo ... flush acts into binlog !
			ctx.reset()
		case "rollback":
			ctx.reset()
		default:
			if isWCommand(req.cmd) {
				ctx.acts = append(ctx.acts, txAction{req.cmd, req.args})
			}
		}
	}

	return err
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
}
