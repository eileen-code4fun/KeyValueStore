package kvs

import (
  "fmt"
  "log"
  "sync"
  "time"
)

const (
  READ = iota
  WRITE

  bufferSize = 100
  rpcTimeout = 100 * time.Millisecond
)

type value struct {
  v string
  ts int
}

type request struct {
  rtype int
  pid int
  k string
  v value
  respChan chan response
}

type response struct {
  pid int
  v value
}

type KVS struct {
  mu sync.Mutex
  running bool
  pid int
  wts int
  store map[string]value
  incoming chan request

  peers map[int]*KVS
}

func NewKVS(pid int) *KVS {
  return &KVS{
    pid: pid,
    wts: 0,
    running: false,
    store: map[string]value{},
  }
}

func (s *KVS) Start(peers map[int]*KVS) {
  s.mu.Lock()
  defer s.mu.Unlock()
  s.peers = peers
  s.running = true
  s.incoming = make(chan request, bufferSize)
  go s.run()
}

func (s *KVS) get(k string) value {
  s.mu.Lock()
  defer s.mu.Unlock()
  return s.store[k]
}

func (s *KVS) put(k string, v value) {
  s.mu.Lock()
  defer s.mu.Unlock()
  s.store[k] = v
}

func (s *KVS) run() {
  for ;s.IsRunning(); {
    var req request
    select {
    case req = <- s.incoming:
      log.Printf("%d received request %v from %d", s.pid, req, req.pid)
    case <-time.After(rpcTimeout):
      continue
    }
    v := s.get(req.k)
    if req.rtype == READ {
      req.respChan <- response{pid: s.pid, v: v}
      log.Printf("%d responded to %v", s.pid, req)
    } else if req.rtype == WRITE {
      if req.v.ts > v.ts || req.v.ts == v.ts && req.pid > s.pid {
        s.put(req.k, req.v)
      }
      req.respChan <- response{pid: s.pid}
      log.Printf("%d responded to %v", s.pid, req)
    } else {
      log.Printf("unknown request %v", req)
    }
  }
}

func (s *KVS) IsRunning() bool {
  s.mu.Lock()
  defer s.mu.Unlock()
  return s.running
}

func (s *KVS) Stop() {
  s.mu.Lock()
  defer s.mu.Unlock()
  s.running = false
  close(s.incoming)
}

func (s *KVS) Put(k, v string) {
  resps := s.broadcast(request{
    rtype: READ,
    pid: s.pid,
    k: k,
  }, s.peers)

  s.mu.Lock()
  wts := s.wts
  for _, resp := range resps {
    if wts < resp.v.ts {
      wts = resp.v.ts
    }
  }
  s.wts = wts + 1
  s.store[k] = value{
    v: v,
    ts: s.wts,
  }
  s.mu.Unlock()

  s.broadcast(request{
    rtype: WRITE,
    pid: s.pid,
    k: k,
    v: s.store[k],
  }, s.peers)
}

func (s *KVS) Get(k string) string {
  resps := s.broadcast(request{
    rtype: READ,
    pid: s.pid,
    k: k,
  }, s.peers)

  s.mu.Lock()
  v := s.store[k]
  for _, resp := range resps {
    if resp.v.ts > v.ts || resp.v.ts == v.ts && resp.pid > s.pid {
      v = resp.v
    }
  }
  s.store[k] = v
  s.mu.Unlock()

  s.broadcast(request{
    rtype: WRITE,
    pid: s.pid,
    k: k,
    v: v,
  }, s.peers)
  return v.v
}

func (s *KVS) broadcast(r request, peers map[int]*KVS) []response {
  log.Printf("%d starts broadcast %v", s.pid, r)
  n := len(peers)/2
  resps := make(chan response, len(peers))
  for i, p := range peers {
    rc := p.incoming
    req := r
    req.respChan = make(chan response, 1)
    id := i
    go func() {
      defer func() {
        // In case the queue is closed.
        if recover() != nil {}
      } ()
      if err := sendHelper(rc, req); err != nil {
        log.Printf("failed to send request to %d; %v", id, err)
        return
      }
      log.Printf("%d sent %v to %d", s.pid, req, id)
      select {
      case resp := <- req.respChan:
        resps <- resp
        log.Printf("%d got response %v from %d", s.pid, resp, id)
      case <-time.After(2 * rpcTimeout):
        log.Printf("%d rpc timeout from %v, %d", s.pid, req, id)
      }
    } ()
  }
  var ret []response
  for i := 0; i < n; i ++ {
    ret = append(ret, <- resps)
  }
  close(resps)
  return ret
}

func sendHelper(q chan request, r request) (err error) {
  defer func() {
    // In case the queue is closed.
    if x := recover(); x != nil {
      err = fmt.Errorf("failed to send the request")
    }
  } ()
  q <- r
  return
}
