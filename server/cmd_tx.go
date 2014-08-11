package server

import (
	"github.com/siddontang/ledisdb/ledis"
)

func beginCommand(req *requestContext) error {
	tx, err := req.db.Begin()
	if err != nil {
		return err
	}

	if err = req.cliCtx.beginTransaction(tx); err != nil {
		tx.Rollback()
		return err
	}

	req.resp.writeStatus(OK)
	return nil
}

func commitCommand(req *requestContext) error {
	var tx *ledis.Tx = req.cliCtx.acquireTx()
	if tx == nil {
		return errTxMiss
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	req.cliCtx.endTransaction()
	req.resp.writeStatus(OK)
	return nil
}

func rollbackCommand(req *requestContext) error {
	var tx *ledis.Tx = req.cliCtx.acquireTx()
	if tx == nil {
		return errTxMiss
	}

	if err := tx.Rollback(); err != nil {
		return err
	}

	req.cliCtx.endTransaction()
	req.resp.writeStatus(OK)
	return nil
}

func init() {
	register("begin", beginCommand)
	register("commit", commitCommand)
	register("rollback", rollbackCommand)
}
