/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Adapted from RocksDB inline skiplist.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.
*/

package utils

import (
	"fmt"
	"log"
	"math"
	"strings"
	"sync/atomic"
	_ "unsafe"

	"github.com/pkg/errors"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

type node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint32
}

type Skiplist struct {
	height     int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	headOffset uint32
	ref        int32
	arena      *Arena
	OnClose    func()
}

// IncrRef increases the refcount
func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}

	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.arena = nil
}

func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOff := arena.putNode(height)
	node := arena.getNode(nodeOff)

	node.height = uint16(height)
	node.keySize = uint16(len(key))
	node.keyOffset = arena.putKey(key)
	node.value = encodeValue(arena.putVal(v), v.EncodedSize())

	return node
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return (uint64(valSize) << 32) | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	return uint32(value), uint32(value >> 32)
}

// NewSkiplist makes a new empty skiplist, with a given arena size
func NewSkiplist(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, ValueStruct{}, maxHeight)
	ho := arena.getNodeOffset(head)
	return &Skiplist{
		height:     1,
		headOffset: ho,
		arena:      arena,
		ref:        1,
	}
}

func (n *node) getValueOffset() (uint32, uint32) {
	val := atomic.LoadUint64(&n.value)
	return decodeValue(val)
}

func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *node) setValue(arena *Arena, vo uint64) {
	atomic.StoreUint64(&n.value, vo)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

// getVs return ValueStruct stored in node
func (n *node) getVs(arena *Arena) ValueStruct {
	return arena.getVal(n.getValueOffset())
}

// Returns true if key is strictly > n.key.
// If n is nil, this is an "end" marker and we return false.
//func (s *Skiplist) keyIsAfterNode(key []byte, n *node) bool {
//	AssertTrue(n != s.head)
//	return n != nil && CompareKeys(key, n.key) > 0
//}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}
	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	nextOffset := nd.getNextOffset(height)
	return s.arena.getNode(nextOffset)
}

func (s *Skiplist) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	prev := s.getHead()
	level := int(s.getHeight() - 1)

	for {
		next := s.getNext(prev, level)
		if next == nil {
			if level != 0 {
				level--
				continue
			}
			if !less {
				return nil, false
			}

			if prev == s.getHead() {
				return nil, false
			}

			return prev, true
		}

		switch CompareKeys(key, next.key(s.arena)) {
		case 0:
			if allowEqual {
				return next, true
			}

			if !less {
				return s.getNext(next, 0), false
			}

			if level != 0 {
				level--
				continue
			}

			if prev == s.getHead() {
				return nil, false
			}
			return prev, true
		case -1:
			if level != 0 {
				level--
				continue
			}

			if !less {
				return next, false
			}

			if prev == s.getHead() {
				return nil, false
			}
			return prev, true
		default:
			prev = next
		}
	}
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return outBefore = outAfter.
// Otherwise, outBefore.key < key < outAfter.key.
func (s *Skiplist) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		prev := s.arena.getNode(before)
		next := s.getNext(prev, level)
		if next == nil {
			return before, prev.getNextOffset(level)
		}

		switch CompareKeys(key, next.key(s.arena)) {
		case 0:
			off := prev.getNextOffset(level)
			return off, off
		case 1:
			before = prev.getNextOffset(level)
		case -1:
			return before, prev.getNextOffset(level)
		}
	}
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// Put inserts the key-value pair.
func (s *Skiplist) Add(e *Entry) {
	key, valueStruct := e.Key, ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}

	currentHeight := int(s.getHeight())
	left := make([]uint32, maxHeight+1)
	right := make([]uint32, maxHeight+1)
	left[currentHeight] = s.headOffset

	for h := currentHeight - 1; h >= 0; h-- {
		left[h], right[h] = s.findSpliceForLevel(key, left[h+1], h)
		if left[h] == right[h] {
			vOff := s.arena.putVal(valueStruct)
			vSize := valueStruct.EncodedSize()
			node := s.arena.getNode(left[h])
			node.setValue(s.arena, encodeValue(vOff, vSize))
			return
		}
	}

	height := s.getHeight()
	newHeight := int32(s.randomHeight())
	for newHeight > height {
		if atomic.CompareAndSwapInt32(&s.height, height, newHeight) {
			break
		}
		height = s.getHeight()
	}

	newN := newNode(s.arena, key, valueStruct, int(newHeight))

	insertHeight := 0
	for insertHeight < int(newHeight) {
		if s.arena.getNode(left[insertHeight]) == nil {
			AssertTrue(insertHeight > 1) // This cannot happen in base level.
			// We haven't computed prev, next for this level because height exceeds old listHeight.
			// For these levels, we expect the lists to be sparse, so we can just search from head.
			left[insertHeight], right[insertHeight] = s.findSpliceForLevel(key, s.headOffset, insertHeight)
			// Someone adds the exact same key before we are able to do so. This can only happen on
			// the base level. But we know we are not on the base level.
			AssertTrue(left[insertHeight] != right[insertHeight])
		}

		prev := s.arena.getNode(left[insertHeight])
		newN.tower[insertHeight] = right[insertHeight]
		if prev.casNextOffset(insertHeight, right[insertHeight], s.arena.getNodeOffset(newN)) {
			insertHeight++
			continue
		}

		left[insertHeight], right[insertHeight] = s.findSpliceForLevel(key, left[insertHeight], insertHeight)
		if right[insertHeight] == left[insertHeight] {
			vOff := s.arena.putVal(valueStruct)
			vSize := valueStruct.EncodedSize()
			prevNode := s.arena.getNode(left[insertHeight])
			prevNode.setValue(s.arena, encodeValue(vOff, vSize))
			return
		}
	}
}

// Empty returns if the Skiplist is empty.
func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (s *Skiplist) findLast() *node {
	currentHeight := int(s.getHeight() - 1)
	prev := s.getHead()
	for {
		next := s.getNext(prev, currentHeight)
		if next != nil {
			prev = next
		}
		if currentHeight != 0 {
			currentHeight--
			continue
		}
		if prev == s.getHead() {
			return nil
		}
		return prev
	}
}

// Get gets the value associated with the key. It returns a valid value if it finds equal or earlier
// version of the same key.
func (s *Skiplist) Search(key []byte) ValueStruct {
	target, result := s.findNear(key, false, true)
	if !result {
		return ValueStruct{}
	}
	if !SameKey(target.key(s.arena), key) {
		return ValueStruct{}
	}
	return target.getVs(s.arena)
}

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (s *Skiplist) NewSkipListIterator() Iterator {
	s.IncrRef()
	return &SkipListIterator{list: s}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal
// arena.
func (s *Skiplist) MemSize() int64 { return s.arena.size() }

// Draw plot Skiplist, align represents align the same node in different level
func (s *Skiplist) Draw(align bool) {
	reverseTree := make([][]string, s.getHeight())
	head := s.getHead()
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		next := head
		for {
			var nodeStr string
			next = s.getNext(next, level)
			if next != nil {
				key := next.key(s.arena)
				vs := next.getVs(s.arena)
				nodeStr = fmt.Sprintf("%s(%s)", key, vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}

	// align
	if align && s.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(s.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := fmt.Sprintf(strings.Repeat("-", len(ele)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}

	// plot
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type SkipListIterator struct {
	list *Skiplist
	n    *node
}

func (s *SkipListIterator) Rewind() {
	s.SeekToFirst()
}

func (s *SkipListIterator) Item() Item {
	return &Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		ExpiresAt: s.Value().ExpiresAt,
		Meta:      s.Value().Meta,
		Version:   s.Value().Version,
	}
}

// Close frees the resources held by the iterator
func (s *SkipListIterator) Close() error {
	s.list.DecrRef()
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *SkipListIterator) Valid() bool {
	return s.n != nil
}

// Key returns the key at the current position.
func (s *SkipListIterator) Key() []byte {
	return s.n.key(s.list.arena)
}

// Value returns value.
func (s *SkipListIterator) Value() ValueStruct {
	return s.n.getVs(s.list.arena)
}

// ValueUint64 returns the uint64 value of the current node.
func (s *SkipListIterator) ValueUint64() uint64 {
	return s.n.value
}

// Next advances to the next position.
func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

// Prev advances to the previous position.
func (s *SkipListIterator) Prev() {
	AssertTrue(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false)
}

// Seek advances to the first entry with a key >= target.
func (s *SkipListIterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true)
}

// SeekForPrev finds an entry with key <= target.
func (s *SkipListIterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, true)
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToLast() {
	s.n = s.list.findLast()
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}

// FastRand is a fast thread local random function.
//go:linkname FastRand runtime.fastrand
func FastRand() uint32

// AssertTruef is AssertTrue with extra info.
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
