package server

import ()

func beginCommand(req *requestContext) error {
	tx, err := req.db.Begin()
	if err != nil {
		return err
	}

	if err = req.cliCtx.beginTx(tx); err != nil {
		tx.Rollback()
		return err
	}

	req.resp.writeStatus(OK)
	return nil
}

func commitCommand(req *requestContext) error {
	ctxTx := req.cliCtx.txCtx
	if ctxTx == nil || ctxTx.tx == nil {
		return errTxMiss
	}

	if err := ctxTx.tx.Commit(); err != nil {
		return err
	}

	req.cliCtx.endTx()
	req.resp.writeStatus(OK)
	return nil
}

func rollbackCommand(req *requestContext) error {
	ctxTx := req.cliCtx.txCtx
	if ctxTx == nil || ctxTx.tx == nil {
		return errTxMiss
	}

	if err := ctxTx.tx.Rollback(); err != nil {
		return err
	}

	req.cliCtx.endTx()
	req.resp.writeStatus(OK)
	return nil
}

func init() {
	register("begin", beginCommand)
	register("commit", commitCommand)
	register("rollback", rollbackCommand)
}
