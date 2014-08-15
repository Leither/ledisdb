package server

import (
	"bufio"
	"errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/ledisdb/ledis"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
)

var errReadRequest = errors.New("invalid request protocol")

type respClient struct {
	app *App

	conn net.Conn

	rb *bufio.Reader

	ctx *clientContext
	req *requestContext
}

type respWriter struct {
	buff *bufio.Writer
}

func newClientRESP(conn net.Conn, app *App) {
	c := new(respClient)

	c.app = app
	c.conn = conn

	c.ctx = newClientContext(app)

	c.rb = bufio.NewReaderSize(conn, 256)

	c.req = c.newRequest()

	go c.run()
}

func (c *respClient) close() error {
	//	ps : async because the call here trace from c.run()
	go func() {
		c.conn.Close()
	}()

	return nil
}

func (c *respClient) invalid() {
	if c.ctx != nil {
		c.ctx.release()
		c.ctx = nil
	}

	c.req = nil
	c.conn = nil
}

func (c *respClient) context() *clientContext {
	return c.ctx
}

func (c *respClient) run() {
	c.app.info.addClients(1)
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]

			log.Fatal("client run panic %s:%v", buf, e)
		}

		if c.conn != nil {
			c.conn.Close()
		}
		c.app.info.addClients(-1)
	}()

	for {
		reqData, err := c.readRequest()
		if err != nil {
			break
		}

		c.handleRequest(reqData)
	}

	c.invalid()
}

func (c *respClient) readLine() ([]byte, error) {
	return ReadLine(c.rb)
}

//A client sends to the Redis server a RESP Array consisting of just Bulk Strings.
func (c *respClient) readRequest() ([][]byte, error) {
	l, err := c.readLine()
	if err != nil {
		return nil, err
	} else if len(l) == 0 || l[0] != '*' {
		return nil, errReadRequest
	}

	var nparams int
	if nparams, err = strconv.Atoi(ledis.String(l[1:])); err != nil {
		return nil, err
	} else if nparams <= 0 {
		return nil, errReadRequest
	}

	req := make([][]byte, 0, nparams)
	var n int
	for i := 0; i < nparams; i++ {
		if l, err = c.readLine(); err != nil {
			return nil, err
		}

		if len(l) == 0 {
			return nil, errReadRequest
		} else if l[0] == '$' {
			//handle resp string
			if n, err = strconv.Atoi(ledis.String(l[1:])); err != nil {
				return nil, err
			} else if n == -1 {
				req = append(req, nil)
			} else {
				buf := make([]byte, n)
				if _, err = io.ReadFull(c.rb, buf); err != nil {
					return nil, err
				}

				if l, err = c.readLine(); err != nil {
					return nil, err
				} else if len(l) != 0 {
					return nil, errors.New("bad bulk string format")
				}

				req = append(req, buf)

			}

		} else {
			return nil, errReadRequest
		}
	}

	return req, nil
}

func (c *respClient) newRequest() *requestContext {
	req := newRequestContext(c.app)

	req.cliCtx = c.ctx
	req.resp = newWriterRESP(c.conn)
	req.remoteAddr = c.conn.RemoteAddr().String()

	return req
}

func (c *respClient) handleRequest(reqData [][]byte) {
	req := c.req

	if len(reqData) == 0 {
		req.cmd = ""
		req.args = reqData[0:0]
	} else {
		req.cmd = strings.ToLower(ledis.String(reqData[0]))
		req.args = reqData[1:]
	}

	req.app.postClientRequest(c, req)
}

//	response writer

func newWriterRESP(conn net.Conn) *respWriter {
	w := new(respWriter)
	w.buff = bufio.NewWriterSize(conn, 256)
	return w
}

func (w *respWriter) writeError(err error) {
	w.buff.Write(ledis.Slice("-ERR"))
	if err != nil {
		w.buff.WriteByte(' ')
		w.buff.Write(ledis.Slice(err.Error()))
	}
	w.buff.Write(Delims)
}

func (w *respWriter) writeStatus(status string) {
	w.buff.WriteByte('+')
	w.buff.Write(ledis.Slice(status))
	w.buff.Write(Delims)
}

func (w *respWriter) writeInteger(n int64) {
	w.buff.WriteByte(':')
	w.buff.Write(ledis.StrPutInt64(n))
	w.buff.Write(Delims)
}

func (w *respWriter) writeBulk(b []byte) {
	w.buff.WriteByte('$')
	if b == nil {
		w.buff.Write(NullBulk)
	} else {
		w.buff.Write(ledis.Slice(strconv.Itoa(len(b))))
		w.buff.Write(Delims)
		w.buff.Write(b)
	}

	w.buff.Write(Delims)
}

func (w *respWriter) writeArray(lst []interface{}) {
	w.buff.WriteByte('*')
	if lst == nil {
		w.buff.Write(NullArray)
		w.buff.Write(Delims)
	} else {
		w.buff.Write(ledis.Slice(strconv.Itoa(len(lst))))
		w.buff.Write(Delims)

		for i := 0; i < len(lst); i++ {
			switch v := lst[i].(type) {
			case []interface{}:
				w.writeArray(v)
			case []byte:
				w.writeBulk(v)
			case nil:
				w.writeBulk(nil)
			case int64:
				w.writeInteger(v)
			default:
				panic("invalid array type")
			}
		}
	}
}

func (w *respWriter) writeSliceArray(lst [][]byte) {
	w.buff.WriteByte('*')
	if lst == nil {
		w.buff.Write(NullArray)
		w.buff.Write(Delims)
	} else {
		w.buff.Write(ledis.Slice(strconv.Itoa(len(lst))))
		w.buff.Write(Delims)

		for i := 0; i < len(lst); i++ {
			w.writeBulk(lst[i])
		}
	}
}

func (w *respWriter) writeFVPairArray(lst []ledis.FVPair) {
	w.buff.WriteByte('*')
	if lst == nil {
		w.buff.Write(NullArray)
		w.buff.Write(Delims)
	} else {
		w.buff.Write(ledis.Slice(strconv.Itoa(len(lst) * 2)))
		w.buff.Write(Delims)

		for i := 0; i < len(lst); i++ {
			w.writeBulk(lst[i].Field)
			w.writeBulk(lst[i].Value)
		}
	}
}

func (w *respWriter) writeScorePairArray(lst []ledis.ScorePair, withScores bool) {
	w.buff.WriteByte('*')
	if lst == nil {
		w.buff.Write(NullArray)
		w.buff.Write(Delims)
	} else {
		if withScores {
			w.buff.Write(ledis.Slice(strconv.Itoa(len(lst) * 2)))
			w.buff.Write(Delims)
		} else {
			w.buff.Write(ledis.Slice(strconv.Itoa(len(lst))))
			w.buff.Write(Delims)

		}

		for i := 0; i < len(lst); i++ {
			w.writeBulk(lst[i].Member)

			if withScores {
				w.writeBulk(ledis.StrPutInt64(lst[i].Score))
			}
		}
	}
}

func (w *respWriter) writeBulkFrom(n int64, rb io.Reader) {
	w.buff.WriteByte('$')
	w.buff.Write(ledis.Slice(strconv.FormatInt(n, 10)))
	w.buff.Write(Delims)

	io.Copy(w.buff, rb)
	w.buff.Write(Delims)
}

func (w *respWriter) flush() {
	w.buff.Flush()
}
