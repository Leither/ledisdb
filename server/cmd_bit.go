package server

import (
	"github.com/siddontang/ledisdb/ledis"
	"strings"
)

func bgetCommand(req *requestContext) error {
	args := req.args
	if len(args) != 1 {
		return ErrCmdParams
	}

	if v, err := req.db.BGet(args[0]); err != nil {
		return err
	} else {
		req.resp.writeBulk(v)
	}
	return nil
}

func bdeleteCommand(req *requestContext) error {
	args := req.args
	if len(args) != 1 {
		return ErrCmdParams
	}

	if n, err := req.db.BDelete(args[0]); err != nil {
		return err
	} else {
		req.resp.writeInteger(n)
	}
	return nil
}

func bsetbitCommand(req *requestContext) error {
	args := req.args
	if len(args) != 3 {
		return ErrCmdParams
	}

	var err error
	var offset int32
	var val int8

	offset, err = ledis.StrInt32(args[1], nil)

	if err != nil {
		return ErrOffset
	}

	val, err = ledis.StrInt8(args[2], nil)
	if val != 0 && val != 1 {
		return ErrBool
	}

	if err != nil {
		return ErrBool
	}

	if ori, err := req.db.BSetBit(args[0], offset, uint8(val)); err != nil {
		return err
	} else {
		req.resp.writeInteger(int64(ori))
	}
	return nil
}

func bgetbitCommand(req *requestContext) error {
	args := req.args
	if len(args) != 2 {
		return ErrCmdParams
	}

	offset, err := ledis.StrInt32(args[1], nil)

	if err != nil {
		return ErrOffset
	}

	if v, err := req.db.BGetBit(args[0], offset); err != nil {
		return err
	} else {
		req.resp.writeInteger(int64(v))
	}
	return nil
}

func bmsetbitCommand(req *requestContext) error {
	args := req.args
	if len(args) < 3 {
		return ErrCmdParams
	}

	key := args[0]
	if len(args[1:])&1 != 0 {
		return ErrCmdParams
	} else {
		args = args[1:]
	}

	var err error
	var offset int32
	var val int8

	pairs := make([]ledis.BitPair, len(args)>>1)
	for i := 0; i < len(pairs); i++ {
		offset, err = ledis.StrInt32(args[i<<1], nil)

		if err != nil {
			return ErrOffset
		}

		val, err = ledis.StrInt8(args[i<<1+1], nil)
		if val != 0 && val != 1 {
			return ErrBool
		}

		if err != nil {
			return ErrBool
		}

		pairs[i].Pos = offset
		pairs[i].Val = uint8(val)
	}

	if place, err := req.db.BMSetBit(key, pairs...); err != nil {
		return err
	} else {
		req.resp.writeInteger(place)
	}
	return nil
}

func bcountCommand(req *requestContext) error {
	args := req.args
	argCnt := len(args)

	if !(argCnt > 0 && argCnt <= 3) {
		return ErrCmdParams
	}

	// BCount(key []byte, start int32, end int32) (cnt int32, err error) {

	var err error
	var start, end int32 = 0, -1

	if argCnt > 1 {
		start, err = ledis.StrInt32(args[1], nil)
		if err != nil {
			return ErrValue
		}
	}

	if argCnt > 2 {
		end, err = ledis.StrInt32(args[2], nil)
		if err != nil {
			return ErrValue
		}
	}

	if cnt, err := req.db.BCount(args[0], start, end); err != nil {
		return err
	} else {
		req.resp.writeInteger(int64(cnt))
	}
	return nil
}

func boptCommand(req *requestContext) error {
	args := req.args
	if len(args) < 2 {
		return ErrCmdParams
	}

	opDesc := strings.ToLower(ledis.String(args[0]))
	dstKey := args[1]
	srcKeys := args[2:]

	var op uint8
	switch opDesc {
	case "and":
		op = ledis.OPand
	case "or":
		op = ledis.OPor
	case "xor":
		op = ledis.OPxor
	case "not":
		op = ledis.OPnot
	default:
		return ErrCmdParams
	}

	if len(srcKeys) == 0 {
		return ErrCmdParams
	}
	if blen, err := req.db.BOperation(op, dstKey, srcKeys...); err != nil {
		return err
	} else {
		req.resp.writeInteger(int64(blen))
	}
	return nil
}

func bexpireCommand(req *requestContext) error {
	args := req.args
	if len(args) != 2 {
		return ErrCmdParams
	}

	duration, err := ledis.StrInt64(args[1], nil)
	if err != nil {
		return ErrValue
	}

	if v, err := req.db.BExpire(args[0], duration); err != nil {
		return err
	} else {
		req.resp.writeInteger(v)
	}

	return nil
}

func bexpireAtCommand(req *requestContext) error {
	args := req.args
	if len(args) != 2 {
		return ErrCmdParams
	}

	when, err := ledis.StrInt64(args[1], nil)
	if err != nil {
		return ErrValue
	}

	if v, err := req.db.BExpireAt(args[0], when); err != nil {
		return err
	} else {
		req.resp.writeInteger(v)
	}

	return nil
}

func bttlCommand(req *requestContext) error {
	args := req.args
	if len(args) != 1 {
		return ErrCmdParams
	}

	if v, err := req.db.BTTL(args[0]); err != nil {
		return err
	} else {
		req.resp.writeInteger(v)
	}

	return nil
}

func bpersistCommand(req *requestContext) error {
	args := req.args
	if len(args) != 1 {
		return ErrCmdParams
	}

	if n, err := req.db.BPersist(args[0]); err != nil {
		return err
	} else {
		req.resp.writeInteger(n)
	}

	return nil
}

func init() {
	register("bget", bgetCommand)
	register("bdelete", bdeleteCommand)
	register("bsetbit", bsetbitCommand)
	register("bgetbit", bgetbitCommand)
	register("bmsetbit", bmsetbitCommand)
	register("bcount", bcountCommand)
	register("bopt", boptCommand)
	register("bexpire", bexpireCommand)
	register("bexpireat", bexpireAtCommand)
	register("bttl", bttlCommand)
	register("bpersist", bpersistCommand)
}
