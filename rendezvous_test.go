package rendezvous_test

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"testing"

	"github.com/tcl326/rendezvous-hashing-go"
)

func newConfig() rendezvous.Config {
	return rendezvous.Config{
		Hasher: hasher{},
	}
}

type testMember string

func (tm testMember) String() string {
	return string(tm)
}

func (tm testMember) Weight() float64 {
	return 100
}

type hasher struct{}

func (hs hasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func TestRendezvousAdd(t *testing.T) {
	cfg := newConfig()
	c := rendezvous.New(nil, cfg)
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members[member.String()] = struct{}{}
		c.Add(member)
	}
	for member := range members {
		found := false
		for _, mem := range c.GetMembers() {
			if member == mem.String() {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s could not be found", member)
		}
	}
}

func TestRendezvousRemove(t *testing.T) {
	members := []rendezvous.WeightedMember{}
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := rendezvous.New(members, cfg)
	if len(c.GetMembers()) != len(members) {
		t.Fatalf("inserted member count is different")
	}
	for _, member := range members {
		c.Remove(member.String())
	}
	if len(c.GetMembers()) != 0 {
		t.Fatalf("member count should be zero")
	}
}

func TestRendezvousLocateKey(t *testing.T) {
	cfg := newConfig()
	c := rendezvous.New(nil, cfg)
	key := []byte("Olric")
	res := c.LocateKey(key)
	if res != nil {
		t.Fatalf("This should be nil: %v", res)
	}
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members[member.String()] = struct{}{}
		c.Add(member)
	}
	res = c.LocateKey(key)
	if res == nil {
		t.Fatalf("This shouldn't be nil: %v", res)
	}
}

func TestRendezvousInsufficientMemberCount(t *testing.T) {
	members := []rendezvous.WeightedMember{}
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := rendezvous.New(members, cfg)
	key := []byte("Olric")
	_, err := c.GetClosestN(key, 30)
	if err != rendezvous.ErrInsufficientMemberCount {
		t.Fatalf("Expected ErrInsufficientMemberCount(%v), Got: %v", rendezvous.ErrInsufficientMemberCount, err)
	}
}

func TestRendezvousClosestMembers(t *testing.T) {
	members := []rendezvous.WeightedMember{}
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := rendezvous.New(members, cfg)
	key := []byte("Olric")
	closestn, err := c.GetClosestN(key, 2)
	if err != nil {
		t.Fatalf("Expected nil, Got: %v", err)
	}
	if len(closestn) != 2 {
		t.Fatalf("Expected closest member count is 2. Got: %d", len(closestn))
	}
	if closestn[0].String() == closestn[1].String() {
		t.Fatalf("The returned two closest member should be different")
	}
}

func BenchmarkAddRemove(b *testing.B) {
	cfg := newConfig()
	c := rendezvous.New(nil, cfg)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := testMember("node" + strconv.Itoa(i))
		c.Add(member)
		c.Remove(member.String())
	}
}

func BenchmarkLocateKey(b *testing.B) {
	cfg := newConfig()
	c := rendezvous.New(nil, cfg)
	c.Add(testMember("node1"))
	c.Add(testMember("node2"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + strconv.Itoa(i))
		c.LocateKey(key)
	}
}

func BenchmarkGetClosestN(b *testing.B) {
	cfg := newConfig()
	c := rendezvous.New(nil, cfg)
	for i := 0; i < 10; i++ {
		c.Add(testMember(fmt.Sprintf("node%d", i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + strconv.Itoa(i))
		c.GetClosestN(key, 3)
	}
}
