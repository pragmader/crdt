package lww

import (
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrElementNotFound occurs when an element with a given key does not exist in the set.
	ErrElementNotFound = errors.New("element not found in the set")
)

// Element contains required operations for a type in order to be used as a set element.
type Element interface {
	// GetKey returns a universally unique identifier (e.g. UUID v4) that can be used
	// to uniquely identify an element across all the replication nodes.
	GetKey() string
}

// IDElement is a simple `Element` implementation that does not carry
// any additional data except its own ID.
type IDElement string

// GetKey implements the `Element` interface
func (e IDElement) GetKey() string {
	return string(e)
}

// addRecord contains an added element and the timestampe when the element was added.
type addRecord struct {
	// Element is the added element
	Element Element
	// Timestamp is when the element was added
	Timestamp time.Time
}

// NewSet initializes the Last-Writer-Wins state-based element set and makes it ready for use.
func NewSet() Set {
	return Set{
		mutex:     &sync.Mutex{},
		additions: make(map[string]addRecord),
		removals:  make(map[string]time.Time),
	}
}

// Set is a Last-Writer-Wins state-based element set implementation.
// Use `NewSet` in order to initialize it before use.
// The set is thread-safe and can be used from several go routines.
type Set struct {
	// mutex is used for the thread-safety
	mutex *sync.Mutex

	// additions is a set of all known additions to the set
	additions map[string]addRecord
	// removals is a set of all known removals from the set
	removals map[string]time.Time
}

// Add adds the given element to the set.
// It replaces an existing element if the element key collides.
func (s *Set) Add(e Element) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// log the addition operation with the current timestamp
	s.additions[e.GetKey()] = addRecord{
		Element:   e,
		Timestamp: time.Now(),
	}
}

// Remove removes an element with the given key from the set.
// This operation succeeds even if the element does not exist in the set.
func (s *Set) Remove(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// log the removal operation with the current timestamp
	s.removals[key] = time.Now()
}

// Replicate takes another LWW Element Set as a `remote` and merges its state into itself.
// Merging two replicas takes the union of their add-sets and remove-sets.
func (s *Set) Replicate(remote Set) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// computing the union of add-sets
	for key, remoteRecord := range remote.additions {
		localRecord, added := s.additions[key]
		if !added || remoteRecord.Timestamp.After(localRecord.Timestamp) {
			s.additions[key] = remoteRecord
		}
	}

	// computing the union of remove-sets
	for key, remoteRemovedAt := range remote.removals {
		localRemovedAt, removed := s.removals[key]
		if !removed || remoteRemovedAt.After(localRemovedAt) {
			s.removals[key] = remoteRemovedAt
		}
	}
}

// Contains checks if an element with the given key exists in the set.
// Returns the found element and no error if the element exists.
// Returns nil and `ErrNotFound` if it does not exist.
func (s Set) Contains(key string) (Element, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Each `Element` is in the set if its `key` is in `additions`,
	// and it is not in `removals` with a higher timestamp.

	addRecord, added := s.additions[key]
	if !added {
		return nil, ErrElementNotFound
	}

	if s.removed(addRecord) {
		return nil, ErrElementNotFound
	}

	return addRecord.Element, nil
}

// List returns a list of the actual elements of the set.
// Because of the internally used map the result order is not deterministic.
func (s Set) List() (list []Element) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// it's always at list an empty list, not nil
	list = []Element{}

	// Each `Element` is in the set if its `key` is in `additions`,
	// and it is not in `removals` with a higher timestamp.
	for _, record := range s.additions {
		if s.removed(record) {
			continue
		}

		list = append(list, record.Element)
	}

	return list
}

// removed returns `true` if the given record is marked as removed
func (s Set) removed(record addRecord) bool {
	removedAt, removed := s.removals[record.Element.GetKey()]
	return removed || removedAt.After(record.Timestamp)
}
