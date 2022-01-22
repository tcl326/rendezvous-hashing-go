package rendezvous

import (
	"math"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/wangjohn/quickselect"
	"github.com/zeebo/xxh3"
)

var FiftyThreeOnes = uint64(0xFFFFFFFFFFFFFFFF >> (64 - 53))
var FiftyThreeZeros = float64(1 << 53)

var ErrInsufficientMemberCount = consistent.ErrInsufficientMemberCount

type Hasher consistent.Hasher

type DefaultHasher struct {
}

func (h *DefaultHasher) Sum64(b []byte) uint64 {
	return xxh3.Hash(b)
}

type WeightedMember interface {
	consistent.Member
	Weight() float64
}

// Config represents a structure to control the rendezvous package.
type Config struct {
	Hasher Hasher
}

// Rendezvous holds the information about the members of the consistent hash circle.
type Rendezvous struct {
	mu sync.RWMutex

	config  Config
	hasher  Hasher
	members map[string]*WeightedMember
	ring    map[uint64]*WeightedMember
}

// New creates and returns a new Rendezvous object
func New(members []WeightedMember, config Config) *Rendezvous {
	r := &Rendezvous{
		config:  config,
		members: make(map[string]*WeightedMember),
		ring:    make(map[uint64]*WeightedMember),
	}

	if config.Hasher == nil {
		// Use the Default Hasher
		r.hasher = &DefaultHasher{}
	} else {
		r.hasher = config.Hasher
	}
	for _, member := range members {
		r.add(member)
	}
	return r
}

// IntToFloat is a golang port of the python implementation mentioned here
// https://en.wikipedia.org/wiki/Rendezvous_hashing#Weighted_rendezvous_hash
func IntToFloat(value uint64) (float_value float64) {
	return float64((value & FiftyThreeOnes)) / FiftyThreeZeros
}

func (r *Rendezvous) ComputeWeightedScore(m WeightedMember, key []byte) (score float64) {
	hash := r.hasher.Sum64(append([]byte(m.String()), key...))
	score = 1.0 / -math.Log(IntToFloat(hash))
	return m.Weight() * score
}

func (r *Rendezvous) LocateKey(key []byte) (member WeightedMember) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	highest_score := -1.0
	for _, _member := range r.members {
		score := r.ComputeWeightedScore(*_member, key)
		if score > highest_score {
			highest_score = score
			member = *_member
		}
	}
	return
}

type byScore []struct {
	string
	float64
}

func (scores byScore) Len() int {
	return len(scores)
}

func (scores byScore) Swap(i, j int) {
	scores[i], scores[j] = scores[j], scores[i]
}

func (scores byScore) Less(i, j int) bool {
	return scores[i].float64 < scores[j].float64
}

// GetClosestN returns the closest N members to the key, the members returned is not sorted
func (r *Rendezvous) GetClosestN(key []byte, count int) (members []WeightedMember, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if count > len(r.members) {
		err = ErrInsufficientMemberCount
		return
	}

	scores := make([]struct {
		string
		float64
	}, len(r.members))
	i := 0
	for member_name, member := range r.members {
		scores[i] = struct {
			string
			float64
		}{member_name, r.ComputeWeightedScore(*member, key)}
		i += 1
	}
	quickselect.QuickSelect(byScore(scores), count)

	members = make([]WeightedMember, count)
	for c := 0; c < count; c++ {
		member_name := scores[c].string
		members[c] = *r.members[member_name]

	}
	return
}

func (r *Rendezvous) add(member WeightedMember) {
	r.members[member.String()] = &member
}

func (r *Rendezvous) Add(member WeightedMember) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[member.String()]; ok {
		// We already have this member. Quit immediately.
		return
	}
	r.add(member)
}

func (r *Rendezvous) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.members[name]; !ok {
		// There is no member with that name. Quit immediately.
		return
	}

	delete(r.members, name)
}

// GetMembers returns a thread-safe copy of members.
func (r *Rendezvous) GetMembers() (members []WeightedMember) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Create a thread-safe copy of member list.
	members = make([]WeightedMember, 0, len(r.members))
	for _, member := range r.members {
		members = append(members, *member)
	}
	return
}
