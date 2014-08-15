package server

import (
	"bufio"
	"bytes"
	//"github.com/siddontang/go-log/log"
	"github.com/siddontang/ledisdb/ledis"
	"io"
	"os"
)

const maxSyncRecords = 64

/*
	response for the context on command replay
*/

type replClient struct {
	app    *App
	cliCtx *clientContext
}

func newReplClient(app *App) *replClient {
	cli := new(replClient)
	cli.app = app
	cli.cliCtx = newClientContext(app)
	return cli
}

func (cli *replClient) close() error {
	if cli.cliCtx != nil {
		cli.cliCtx.release()
		cli.cliCtx = nil
	}
	return nil
}

func (cli *replClient) context() *clientContext {
	return cli.cliCtx
}

func (cli *replClient) writeError(err error)                              {}
func (cli *replClient) writeStatus(sz string)                             {}
func (cli *replClient) writeInteger(n int64)                              {}
func (cli *replClient) writeBulk(sz []byte)                               {}
func (cli *replClient) writeArray(lst []interface{})                      {}
func (cli *replClient) writeSliceArray(lst [][]byte)                      {}
func (cli *replClient) writeFVPairArray(lst []ledis.FVPair)               {}
func (cli *replClient) writeScorePairArray(lst []ledis.ScorePair, b bool) {}
func (cli *replClient) writeBulkFrom(n int64, r io.Reader)                {}
func (cli *replClient) flush()                                            {}

/*
	replication entry
*/

type replication struct {
	app *App
	m   *master

	cli    *replClient
	replay *requestContext
	aofRcd *aofRecord
	aof    *Aof
}

func newReplication(app *App) *replication {
	repl := new(replication)
	repl.app = app
	repl.m = new(master)

	repl.cli = newReplClient(app)
	repl.aofRcd = new(aofRecord)
	repl.aof = app.aof

	return repl
}

func (repl *replication) newReplayRequest(rcd *aofRecord) *requestContext {
	replay := repl.replay
	if replay == nil {
		replay = newRequestContext(repl.app)
		replay.app = repl.app
		replay.cliCtx = repl.cli.cliCtx
		replay.resp = repl.cli
		replay.remoteAddr = "replication"

		repl.replay = replay
	}

	//	set relay db
	if err := replay.cliCtx.changeDB(rcd.db); err != nil {
		return nil
	}

	//	set command
	elements := bytes.Split(rcd.fullcmd, []byte(" "))

	repl.replay.cmd = ledis.String(elements[0])

	if len(elements) > 1 {
		repl.replay.args = elements[1:]
	} else {
		repl.replay.args = elements[0:0]
	}

	return repl.replay
}

func (repl *replication) replicateRecord(ctime uint32, rcd *aofRecord) error {
	replay := repl.newReplayRequest(rcd)
	// err := repl.app.postClientRequest(repl.cli, repl.replay)
	// if err != nil {
	// 	log.Fatal("replication error %s, skip to next", err.Error())
	// 	return ErrSkipEvent
	// }

	repl.app.postClientRequest(repl.cli, replay)

	//	err skip record ??

	return nil
}

func (repl *replication) ReplicateFromReader(r io.Reader) error {
	if _, err := AofReadN(r, repl.replicateRecord, -1); err != nil {
		return err
	}
	return nil
}

func (repl *replication) ReplicateFromData(data []byte) error {
	rb := bytes.NewReader(data)
	return repl.ReplicateFromReader(rb)
}

func (repl *replication) ReplicateFromFile(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}

	rb := bufio.NewReaderSize(f, 4096)
	err = repl.ReplicateFromReader(rb)

	f.Close()

	return err
}

// func (repl *replication) Read(anchor *aofAnchor, w io.Writer) {
// 	place := func(ctime uint32, data bytes.Buffer) error {
// 		if _, err := io.CopyN(w, &data, int64(data.Len())); err != nil {
// 			return err
// 		}

// 		if err := repl.aofRcd.setup(data.Bytes()); err != nil {
// 			return
// 		}

// 		err := repl.replicateRecord(ctime, rcd)
// 		if err != nil {
// 			log.Fatal("replication error %s, skip to next", err.Error())
// 			return ErrSkipEvent
// 		}
// 		return nil
// 	}

// 	return repl.aof.Read(anchor, maxSyncRecords, place)
// }
