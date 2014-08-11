package server

import (
	"encoding/json"
	"fmt"
	"github.com/siddontang/ledisdb/ledis"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
)

type info struct {
	sync.Mutex
	Server struct {
		OS         string `json:"os"`
		ProceessId int    `json:"process_id"`
		RespAddr   string `json:"resp_addr"`
		HttpAddr   string `json:"http_addr"`

		GoroutineNum int `json:"goroutine_num"`
	} `json:"Server"`

	Clients struct {
		ConnectedClients int64 `json:"connected_clients"`
	} `json:"Clients"`

	Memory struct {
		MemoryAlloc      uint64 `json:"memory_alloc"`
		MemoryAllocHuman string `json:"memory_alloc_human"`
	} `json:"Memory"`

	Cpu struct {
		UsedCpuSys  int64 `json:"used_cpu_sys"`
		UsedCpuUser int64 `json:"used_cpu_user"`
	} `json:"CPU"`

	Persistence struct {
		StoreBackend    string `json:"store_backend"`
		Compression     bool   `json:"compression"`
		BlockSize       int    `json:"block_size"`
		WriteBufferSize int    `json:"write_buffer_size"`
		CacheSize       int    `json:"cache_size"`
		MaxOpenFiles    int    `json:"max_open_files"`
		MapSize         int    `json:"map_size"`
	} `json:"Persistence"`

	Keyspace map[string]*ledis.Keyspace `json:"Keyspace"`
}

func newInfo(app *App) *info {
	i := new(info)

	i.Server.OS, _ = getOS()
	i.Server.ProceessId = os.Getpid()
	i.Server.RespAddr = app.cfg.Addr
	i.Server.HttpAddr = app.cfg.HttpAddr

	i.Clients.ConnectedClients = 0

	i.Memory.MemoryAlloc = 0
	i.Memory.MemoryAllocHuman = ""

	i.Persistence.StoreBackend = app.cfg.DB.Name
	i.Persistence.Compression = app.cfg.DB.Compression
	i.Persistence.BlockSize = app.cfg.DB.BlockSize
	i.Persistence.WriteBufferSize = app.cfg.DB.WriteBufferSize
	i.Persistence.CacheSize = app.cfg.DB.CacheSize
	i.Persistence.MaxOpenFiles = app.cfg.DB.MaxOpenFiles
	i.Persistence.MapSize = app.cfg.DB.MapSize

	i.Keyspace = make(map[string]*ledis.Keyspace)
	for idx := 0; idx < int(ledis.MaxDBNumber); idx++ {
		db, err := app.ldb.Select(idx)
		if err != nil {
			panic(err)
			return nil
		}
		dbStr := fmt.Sprintf("db%d", idx)
		i.Keyspace[dbStr] = db.Keyspace
	}
	return i
}

func (i *info) addClients(delta int64) {
	atomic.AddInt64(&i.Clients.ConnectedClients, delta)
}

func (i *info) collectSysInfo() {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(i.Server.ProceessId, &rusage); err != nil {
		return
	}
	i.Lock()
	i.Cpu.UsedCpuSys = rusage.Stime.Sec
	i.Cpu.UsedCpuUser = rusage.Utime.Sec

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	i.Memory.MemoryAlloc = mem.Alloc
	i.Memory.MemoryAllocHuman = getMemoryHuman(mem.Alloc)

	i.Server.GoroutineNum = runtime.NumGoroutine()

	i.Unlock()
}

func getMemoryHuman(m uint64) string {
	if m > 1024*1024*1024 {
		return fmt.Sprintf("%.2fG", m/1024/1024/1024.0)
	} else if m > 1024*1024 {
		return fmt.Sprintf("%.2fM", m/1024/1024.0)
	} else if m > 1024 {
		return fmt.Sprintf("%.2fK", m/1024.0)
	} else {
		return fmt.Sprintf("%d", m)
	}
}

func getOS() (string, error) {
	var uname syscall.Utsname
	if err := syscall.Uname(&uname); err != nil {
		return "", err
	}
	str := fmt.Sprintf("%s %s %s",
		arr2str(uname.Sysname),
		arr2str(uname.Release),
		arr2str(uname.Machine),
	)
	return str, nil
}

func arr2str(arr [65]int8) string {
	var buf []byte
	for _, c := range arr {
		if c != 0 {
			buf = append(buf, byte(c))
		}
	}
	return string(buf)
}

func (i *info) dumps(section string) ([]byte, error) {
	i.collectSysInfo()

	switch section {
	case "":
		return json.Marshal(i)

	case "server":
		return json.Marshal(map[string]interface{}{
			"Server": i.Server,
		})
	case "memory":
		return json.Marshal(map[string]interface{}{
			"Memory": i.Memory,
		})
	case "cpu":
		return json.Marshal(map[string]interface{}{
			"CPU": i.Cpu,
		})
	case "persistence":
		return json.Marshal(map[string]interface{}{
			"Persistence": i.Persistence,
		})
	case "keyspace":
		return json.Marshal(map[string]interface{}{
			"Keyspace": i.Keyspace,
		})
	}
	return []byte(""), nil
}
