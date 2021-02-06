package kvs
// go test -v -run Test*

import (
  "fmt"
  "strings"
  "sync"
  "testing"
)

func TestKVSSingleton(t *testing.T) {
  s := NewKVS(0)
  s.Start(nil)
  s.Put("k1", "v1")
  s.Put("k2", "v2")
  if v := s.Get("k1"); v != "v1" {
    t.Errorf("got %s for key k1; want v1", v)
  }
  if v := s.Get("k2"); v != "v2" {
    t.Errorf("got %s for key k2; want v2", v)
  }
  s.Put("k1", "vv")
  if v := s.Get("k1"); v != "vv" {
    t.Errorf("got %s for key k1; want vv", v)
  }
  s.Stop()
}

func TestKVSSync(t *testing.T) {
  s0 := NewKVS(0)
  s1 := NewKVS(1)
  s2 := NewKVS(2)
  s0.Start(map[int]*KVS{1: s1, 2: s2})
  s1.Start(map[int]*KVS{0: s0, 2: s2})
  s2.Start(map[int]*KVS{0: s0, 1: s1})
  s0.Put("k0", "v0")
  s1.Put("k1", "v1")
  s2.Put("k2", "v2")
  s2.Put("k0", "v00")
  s0.Put("k1", "v11")
  s1.Put("k2", "v22")
  if v := s0.Get("k0"); v != "v00" {
    t.Errorf("got %s for key k0; want v00", v)
  }
  if v := s1.Get("k1"); v != "v11" {
    t.Errorf("got %s for key k1; want v11", v)
  }
  if v := s2.Get("k2"); v != "v22" {
    t.Errorf("got %s for key k2; want v22", v)
  }
  s0.Stop()
  s1.Stop()
  s2.Stop()
}

func TestKVSFaultTolerant(t *testing.T) {
  s0 := NewKVS(0)
  s1 := NewKVS(1)
  s2 := NewKVS(2)
  s0.Start(map[int]*KVS{1: s1, 2: s2})
  s1.Start(map[int]*KVS{0: s0, 2: s2})
  s2.Start(map[int]*KVS{0: s0, 1: s1})
  s2.Stop()
  s0.Put("k0", "v0")
  s2.Start(map[int]*KVS{0: s0, 1: s1})
  s0.Stop()
  s1.Put("k1", "v1")
  s0.Start(map[int]*KVS{1: s1, 2: s2})
  s1.Stop()
  s2.Put("k2", "v2")
  s1.Start(map[int]*KVS{0: s0, 2: s2})
  // s2 was down when k0 was set.
  if v := s2.Get("k0"); v != "v0" {
    t.Errorf("got %s for key k0; want v0", v)
  }
  // s0 was done when k1 was set.
  if v := s0.Get("k1"); v != "v1" {
    t.Errorf("got %s for key k1; want v1", v)
  }
  // s1 was down when k2 was set.
  if v := s1.Get("k2"); v != "v2" {
    t.Errorf("got %s for key k2; want v2", v)
  }
  s0.Stop()
  s1.Stop()
  s2.Stop()
}

func TestKVSConcurrent(t *testing.T) {
  s0 := NewKVS(0)
  s1 := NewKVS(1)
  s2 := NewKVS(2)
  s0.Start(map[int]*KVS{1: s1, 2: s2})
  s1.Start(map[int]*KVS{0: s0, 2: s2})
  s2.Start(map[int]*KVS{0: s0, 1: s1})
  total := 10
  var wg sync.WaitGroup
  for _, s := range []*KVS{s0, s1, s2} {
    wg.Add(1)
    kvs := s
    go func() {
      for i := 0; i < total; i ++ {
        kvs.Put(fmt.Sprintf("%d", i), fmt.Sprintf("%d_%d", i, kvs.pid))
      }
      wg.Done()
    } ()
  }
  wg.Wait()
  for i := 0; i < total; i ++ {
    v := s0.Get(fmt.Sprintf("%d", i))
    if !strings.HasPrefix(v, fmt.Sprintf("%d_", i)) {
      t.Errorf("got value %s for key %d; want it to start with %d", v, i, i)
    }
  }
  s0.Stop()
  s1.Stop()
  s2.Stop()
}
