// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package lsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/pb"
	"github.com/hardcore-os/corekv/utils"
	"github.com/pkg/errors"
	"io"
	"os"
	"sort"
	"unsafe"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

type block struct {
	offset            int //当前block的offset 首地址
	checksum          []byte
	entriesIndexStart int
	chkLen            int
	data              []byte
	baseKey           []byte
	entryOffsets      []uint32
	end               int
	estimateSz        int64
}

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Decode decodes the header.
func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func (tb *tableBuilder) add(entry *utils.Entry, isStale bool) {
	key := entry.Key
	value := &utils.ValueStruct{
		Meta:      entry.Meta,
		ExpiresAt: entry.ExpiresAt,
		Value:     entry.Value,
	}
	if tb.tryFinishBlock(entry) {
		if isStale {
			tb.staleDataSize += len(entry.Key) + 4 /* len */ + 4 /* offset */
		}

		tb.finishBlock()
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize),
		}
	}

	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = key
	}
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))
	diffKey, sameNum := calculateDiffKey(tb.baseKey, key)
	h := &header{
		overlap: uint16(sameNum),
		diff:    uint16(len(diffKey)),
	}
	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))
	tb.append(h.encode())
	tb.append(diffKey)

	bytedVal := make([]byte, value.EncodedSize())
	value.EncodeValue(bytedVal)
	tb.append(bytedVal)
}

func calculateDiffKey(baseKey []byte, curKey []byte) ([]byte, int) {
	index := 0
	for index < len(baseKey) && index < len(curKey) && baseKey[index] == curKey[index] {
		index++
	}
	return curKey[index+1:], index + 1
}

func (tb *tableBuilder) finishBlock() {
	curBlk := tb.curBlock
	if curBlk == nil {
		return
	}

	bytedOffset := utils.U32SliceToBytes(curBlk.entryOffsets)
	tb.append(bytedOffset)
	tb.append(utils.U32ToBytes(uint32(len(curBlk.entryOffsets))))

	checkSum := utils.CalculateChecksum(curBlk.data)
	bytedCheckSum := utils.U32ToBytes(uint32(checkSum))
	sumLen := len(bytedCheckSum)
	tb.append(bytedCheckSum)
	tb.append(utils.U32ToBytes(uint32(sumLen)))

	tb.estimateSz += curBlk.estimateSz
	tb.blockList = append(tb.blockList, curBlk)
	tb.keyCount += uint32(len(curBlk.entryOffsets))
	tb.curBlock = nil
}

func (tb *tableBuilder) append(data []byte) {
	copied := tb.allocate(data)
	utils.CondPanic(copied != len(data), errors.New("tableBuilder.append data"))
}

func (tb *tableBuilder) allocate(data []byte) int {
	curBlk := tb.curBlock
	need := len(data)
	if len(curBlk.data[curBlk.end:]) < need {
		sz := len(curBlk.data) * 2
		if sz < curBlk.end+need {
			sz = curBlk.end + need
		}
		tmp := make([]byte, sz)
		copy(tmp, curBlk.data)
		curBlk.data = tmp
	}
	curBlk.end += need
	return copy(curBlk.data[curBlk.end:], data)
}

func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	curBlk := tb.curBlock
	if curBlk == nil {
		return true
	}

	if len(curBlk.data) == 0 {
		return false
	}

	estimateSize := int64((len(curBlk.entryOffsets)+1)*4) + 8 /*checkSum*/ + 4 /*checkSumLen*/ + 4 /*offset*/
	curBlk.estimateSz += 6 + int64(len(e.Key)) + int64(e.EncodedSize()) + int64(curBlk.end) + estimateSize

	return curBlk.estimateSz > int64(tb.opt.BlockSize)
}

func newTableBuilerWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}
func newTableBuiler(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	bd := tb.done()
	t = &table{lm: lm, fid: utils.FID(tableName)}
	// 如果没有builder 则创打开一个已经存在的sst文件
	t.ss = file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size)})
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

// Empty returns whether it's empty.
func (tb *tableBuilder) empty() bool { return len(tb.keyHashes) == 0 }

func (bd *buildData) Copy(dst []byte) int {
	written := 0
	for _, b := range bd.blockList {
		written += copy(dst[written:], b.data[:b.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

func (tb *tableBuilder) done() *buildData {
	tb.finishBlock()
	bd := &buildData{}
	bd.blockList = tb.blockList

	var f utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}

	indexData, dataSize := tb.buildIndexBlock(f)
	bd.index = indexData

	bd.checksum = utils.U64ToBytes(utils.CalculateChecksum(indexData))
	bd.size = int(dataSize) + len(indexData) + len(bd.checksum) + 4 + 4
	return bd
}

func (tb *tableBuilder) buildIndexBlock(filter []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(filter) > 0 {
		tableIndex.BloomFilter = filter
	}

	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.KeyCount = tb.keyCount
	tableIndex.Offsets = tb.buildOffsetList()

	var dataSize uint32
	for _, b := range tb.blockList {
		dataSize += uint32(b.end)
	}

	rawTbIndex, err := json.Marshal(tableIndex)
	utils.Panic(err)

	return rawTbIndex, dataSize
}

func (tb *tableBuilder) buildOffsetList() []*pb.BlockOffset {
	var res []*pb.BlockOffset
	offset := uint32(0)
	for _, b := range tb.blockList {
		bo := &pb.BlockOffset{
			Key:    b.baseKey,
			Offset: offset,
			Len:    uint32(b.end),
		}
		res = append(res, bo)
		offset += uint32(b.end)
	}
	return res
}

func (b *tableBuilder) ReachedCapacity() bool {
	return b.estimateSz > b.sstSize
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	data         []byte
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	prevOverlap uint16

	it utils.Item
}

func (bi *blockIterator) setBlock(block *block) {
	bi.data = block.data[:block.entriesIndexStart]
	bi.idx = 0
	bi.err = nil
	bi.baseKey = bi.baseKey[:0]
	bi.key = bi.key[:0]
	bi.val = bi.val[:0]
	bi.entryOffsets = block.entryOffsets
	bi.block = block
	bi.prevOverlap = 0

	var h header
	h.decode(bi.data)
	bi.baseKey = bi.data[headerSize : headerSize+h.diff]
}

func (bi *blockIterator) seekToFirst() {
	bi.setIdx(0)
}

func (bi *blockIterator) seekToLast() {
	bi.setIdx(len(bi.entryOffsets) - 1)
}

func (bi *blockIterator) seek(key []byte) {
	startIndex := 0
	bi.err = nil
	foundEntryIndex := sort.Search(len(bi.entryOffsets), func(idx int) bool {
		if idx < startIndex {
			return false
		}
		bi.setIdx(idx)
		return utils.CompareKeys(bi.key, key) >= 0
	})
	bi.setIdx(foundEntryIndex)
}

func (bi *blockIterator) setIdx(i int) {
	if i < 0 || i >= len(bi.entryOffsets) {
		bi.err = io.EOF
		return
	}
	bi.err = nil
	bi.idx = i

	startOffset := bi.entryOffsets[i]
	var endOffset uint32
	if i+1 == len(bi.entryOffsets) {
		endOffset = uint32(bi.block.entriesIndexStart)
	} else {
		endOffset = bi.entryOffsets[i+1]
	}

	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				bi.tableID, bi.blockID, bi.idx, len(bi.data), startOffset, endOffset,
				len(bi.entryOffsets), bi.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	entryData := bi.data[startOffset:endOffset]
	var h header
	h.decode(entryData)

	if bi.prevOverlap < h.overlap {
		bi.key = append(bi.key[:bi.prevOverlap], bi.baseKey[bi.prevOverlap:h.overlap]...)
	}

	bi.prevOverlap = h.overlap
	bi.key = append(bi.key, entryData[headerSize:headerSize+h.diff]...)

	value := &utils.ValueStruct{}
	value.DecodeValue(entryData[headerSize+h.diff:])

	bi.val = value.Value
	e := &utils.Entry{
		Key:       bi.key,
		Value:     bi.val,
		ExpiresAt: value.ExpiresAt,
		Meta:      value.Meta,
	}

	bi.it = &Item{e: e}
}

func (bi *blockIterator) Error() error {
	return bi.err
}

func (bi *blockIterator) Next() {
	bi.setIdx(bi.idx + 1)
}

func (bi *blockIterator) Valid() bool {
	return bi.err != io.EOF
}

func (bi *blockIterator) Item() utils.Item {
	return bi.it
}

func (itr *blockIterator) Rewind() bool {
	itr.setIdx(0)
	return true
}

func (itr *blockIterator) Close() error {
	return nil
}