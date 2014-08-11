package server

import (
	"fmt"
	"github.com/siddontang/ledisdb/ledis"
	"strconv"
	"strings"
)

type CommandFunc func(req *requestContext) error

var regCmds = map[string]CommandFunc{}
var wCmds = map[string]byte{}

func register(name string, f CommandFunc) {
	if _, ok := regCmds[strings.ToLower(name)]; ok {
		panic(fmt.Sprintf("%s has been registered", name))
	}

	regCmds[name] = f
}

func pingCommand(req *requestContext) error {
	req.resp.writeStatus(PONG)
	return nil
}

func echoCommand(req *requestContext) error {
	if len(req.args) != 1 {
		return ErrCmdParams
	}

	req.resp.writeBulk(req.args[0])
	return nil
}

func selectCommand(req *requestContext) error {
	if len(req.args) != 1 {
		return ErrCmdParams
	}

	if index, err := strconv.Atoi(ledis.String(req.args[0])); err != nil {
		return err
	} else {
		if db, err := req.ldb.Select(index); err != nil {
			return err
		} else {
			req.cliCtx.db = db
			req.resp.writeStatus(OK)
		}
	}

	return nil
}

func loadWCommands() {
	for _, cmd := range cnfCmds {
		if !cmd.readonly {
			wCmds[strings.ToLower(cmd.name)] = 1
		}
	}
}

func isWCommand(cmdName string) bool {
	_, ok := wCmds[cmdName]
	return ok
}

func init() {
	loadWCommands()

	register("ping", pingCommand)
	register("echo", echoCommand)
	register("select", selectCommand)
}
