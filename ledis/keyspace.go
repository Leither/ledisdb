package ledis

import ()

type Keyspace struct {
	Kvs       int `json:"kvs"`
	KvExpires int `json:"kv_expires"`

	Lists       int `json:"lists"`
	ListExpires int `json:"list_expires"`

	Bitmaps       int `json:"bitmaps"`
	BitmapExpires int `json:"bitmap_expires"`

	ZSets       int `json:"zsets"`
	ZSetExpires int `json:"zset_expires"`

	Hashes      int `json:"hashes"`
	HashExpires int `json:"hahsh_expires"`
}

func (k *Keyspace) add(dataType byte, extraDataType byte, delta int) {
	switch dataType {
	case KVType:
		k.Kvs += delta
	case LMetaType:
		k.Lists += delta
	case BitMetaType:
		k.Bitmaps += delta
	case ZSizeType:
		k.ZSets += delta
	case HSizeType:
		k.Hashes += delta
	case ExpMetaType:
		k.addExpires(extraDataType, delta)
	}
}

func (k *Keyspace) addExpires(dataType byte, delta int) {
	switch dataType {
	case KVType:
		k.KvExpires += delta
	case ListType:
		k.ListExpires += delta
	case BitType:
		k.BitmapExpires += delta
	case ZSetType:
		k.ZSetExpires += delta
	case HashType:
		k.HashExpires += delta
	}
}
