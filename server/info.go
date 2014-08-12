package server

import (
	"encoding/json"
	"fmt"
	"github.com/siddontang/ledisdb/ledis"
	"github.com/siddontang/ledisdb/store"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var writeFileInterval = 10 * time.Second

type info struct {
	sync.Mutex
	quit   chan struct{} `json:"-"`
	file   *os.File      `json:"-"`
	Server struct {
		OS           string `json:"os"`
		ProceessId   int    `json:"process_id"`
		RespAddr     string `json:"resp_addr"`
		HttpAddr     string `json:"http_addr"`
		GoroutineNum int    `json:"goroutine_num"`
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
		StoreBackend string `json:"store_backend"`
	} `json:"Persistence"`

	Keyspace []*ledis.Keyspace `json:"Keyspace"`
}

func newInfo(app *App) (i *info, err error) {
	i = new(info)

	dataDir := app.cfg.DataDir
	fileName := filepath.Join(dataDir, "info.json")

	i.Keyspace = make([]*ledis.Keyspace, ledis.MaxDBNumber)
	for idx := 0; idx < int(ledis.MaxDBNumber); idx++ {
		i.Keyspace[idx] = &ledis.Keyspace{}
	}
	if err := i.loads(fileName); err != nil {
	}
	for idx := 0; idx < int(ledis.MaxDBNumber); idx++ {
		db, err := app.ldb.Select(idx)
		if err != nil {
			return nil, err
		}
		db.Keyspace = i.Keyspace[idx]
	}
	if i.file, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666); err != nil {
		return nil, err
	}

	i.Server.OS, _ = getOS()
	i.Server.ProceessId = os.Getpid()
	i.Server.RespAddr = app.cfg.Addr
	i.Server.HttpAddr = app.cfg.HttpAddr
	i.Memory.MemoryAllocHuman = ""

	if app.cfg.DB.Name != "" {
		i.Persistence.StoreBackend = app.cfg.DB.Name
	} else {
		i.Persistence.StoreBackend = store.DefaultStoreName
	}
	go func() {
		t := time.NewTicker(writeFileInterval)
		end := false
		for !end {
			select {
			case <-t.C:
				i.writeFile(false)
			case <-i.quit:
				if err := i.writeFile(true); err != nil {
					return
				}
				end = true
				break
			}
		}
		i.quit <- struct{}{}
	}()
	return i, nil
}

func (i *info) addClients(delta int64) {
	atomic.AddInt64(&i.Clients.ConnectedClients, delta)
}
func (i *info) Close() {
	i.quit <- struct{}{}
	<-i.quit
}

func (i *info) collectSysInfo() {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return
	}
	i.Lock()
	i.Cpu.UsedCpuSys = rusage.Stime.Usec
	i.Cpu.UsedCpuUser = rusage.Utime.Usec

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	i.Memory.MemoryAlloc = mem.Alloc
	i.Memory.MemoryAllocHuman = getMemoryHuman(mem.Alloc)

	i.Server.GoroutineNum = runtime.NumGoroutine()
	i.Unlock()
}

func getMemoryHuman(m uint64) string {
	var gb uint64 = 1024 * 1024 * 1024
	var mb uint64 = 1024 * 1024
	var kb uint64 = 1024
	if m > gb {
		return fmt.Sprintf("%dG", m/gb)
	} else if m > mb {
		return fmt.Sprintf("%dM", m/mb)
	} else if m > kb {
		return fmt.Sprintf("%dK", m/kb)
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

func (i *info) writeFile(flush bool) error {
	content, err := i.dumps("keyspace")
	if err != nil {
		return err
	}
	if err := i.file.Truncate(0); err != nil {
		return err
	}
	if _, err := i.file.Seek(0, os.SEEK_SET); err != nil {
		return err
	}
	if _, err := i.file.Write(content); err != nil {
		return err
	}
	if flush {
		i.file.Sync()
		i.file.Close()
	}
	return nil
}

func (i *info) loads(fileName string) error {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	return json.Unmarshal(content, &i.Keyspace)
}

func (i *info) dumps(section string) ([]byte, error) {
	i.collectSysInfo()

	switch section {
	case "":
		return json.Marshal(i)

	case "server":
		return json.Marshal(i.Server)
	case "memory":
		return json.Marshal(i.Memory)
	case "cpu":
		return json.Marshal(i.Cpu)
	case "persistence":
		return json.Marshal(i.Persistence)
	case "keyspace":
		return json.Marshal(i.Keyspace)
	}
	return []byte(""), nil
}
