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
	"encoding/binary"
	"fmt"
	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/pb"
	"github.com/hardcore-os/corekv/utils"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	// 对builder存在的情况 把buf flush到磁盘
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		// 如果没有builder 则创打开一个已经存在的sst文件
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize)})
	}
	// 先要引用一下，否则后面使用迭代器会导致引用状态错误
	t.IncrRef()
	//  初始化sst文件，把index加载进来
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	// 获取sst的最大key 需要使用迭代器
	itr := t.NewIterator(&utils.Options{}) // 默认是降序
	defer itr.Close()
	// 定位到初始位置就是最大的key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)

	return t
}

func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}

	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	var ko pb.BlockOffset
	ok = t.offsets(&ko, idx)
	if !ok {
		return nil, fmt.Errorf("block t.offset id=%d", idx)
	}

	bk := &block{
		offset: int(ko.Offset),
	}

	var err error
	bk.data, err = t.read(int(ko.Offset), int(ko.Len))
	if err != nil {
		return nil, err
	}

	readPos := len(bk.data) - 4
	a := utils.BytesToU32(bk.data[readPos : readPos+4])
	bk.chkLen = int(a)
	readPos -= bk.chkLen
	bk.checksum = bk.data[readPos : readPos+bk.chkLen]

	bk.data = bk.data[:readPos]
	if err := bk.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	entriesLen := int(utils.BytesToU32(bk.data[readPos : readPos+4]))
	startOffsets := readPos - 4*entriesLen
	endOffsets := startOffsets + 4*entriesLen

	bk.entriesIndexStart = startOffsets
	bk.entryOffsets = utils.BytesToU32Slice(bk.data[startOffsets:endOffsets])

	t.lm.cache.blocks.Set(key, bk)
	return bk, nil
}

func (t *table) Serach(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()

	indexs := t.ss.Indexs()
	filter := utils.Filter(indexs.BloomFilter)
	if t.ss.HasBloomFilter() && !filter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}

	titer := t.NewIterator(&utils.Options{})
	titer.Seek(key)

	if !titer.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if utils.SameKey(titer.it.Entry().Key, key) {
		if version := utils.ParseTs(titer.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return titer.Item().Entry(), nil
		}
	}

	return nil, utils.ErrKeyNotFound
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*ko = *index.GetOffsets()[i]
	return true
}

func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	// Assume t.ID does not overflow uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *table) Delete() error {
	return t.ss.Detele()
}

func (t *table) indexKey() uint64 {
	return t.fid
}

func (t *table) getEntry(key, block []byte, idx int) (entry *utils.Entry, err error) {
	if len(block) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	dataStr := string(block)
	blocks := strings.Split(dataStr, ",")
	if idx >= 0 && idx < len(blocks) {
		return &utils.Entry{
			Key:   key,
			Value: []byte(blocks[idx]),
		}, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// TODO 从缓存中删除
		for i := 0; i < len(t.ss.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) Size() int64 { return int64(t.ss.Size()) }

// GetCreatedAt
func (t *table) GetCreatedAt() *time.Time {
	return t.ss.GetCreatedAt()
}

func (t *table) StaleDataSize() uint32 { return t.ss.Indexs().StaleDataSize }

func (t *table) NewIterator(opt *utils.Options) *tableIterator {
	t.IncrRef()
	ti := &tableIterator{
		opt: opt,
		t:   t,
		bi:  &blockIterator{},
	}
	return ti
}

func (ti *tableIterator) Next() {
	ti.err = nil
	if ti.blockPos == len(ti.t.ss.Indexs().GetOffsets()) {
		ti.err = io.EOF
		return
	}

	if len(ti.bi.data) == 0 {
		b, err := ti.t.block(ti.blockPos)
		if err != nil {
			ti.err = err
			return
		}
		ti.bi.tableID = ti.t.fid
		ti.bi.blockID = ti.blockPos
		ti.bi.setBlock(b)
		ti.bi.seekToFirst()
		ti.err = ti.bi.Error()
		return
	}

	ti.bi.Next()
	if !ti.bi.Valid() {
		ti.blockPos++
		ti.bi.data = nil
		ti.Next()
		return
	}
	ti.it = ti.bi.Item()
}

func (ti *tableIterator) Valid() bool {
	return ti.err != io.EOF
}

func (it *tableIterator) Item() utils.Item {
	return it.it
}

func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
}

func (ti *tableIterator) SeekToFirst() {
	numBlocks := len(ti.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		ti.err = io.EOF
		return
	}
	ti.blockPos = 0
	b, err := ti.t.block(ti.blockPos)
	if err != nil {
		ti.err = err
		return
	}
	ti.bi.tableID = ti.t.fid
	ti.bi.blockID = ti.blockPos
	ti.bi.setBlock(b)
	ti.bi.seekToFirst()
	ti.it = ti.bi.Item()
	ti.err = ti.bi.Error()
}

func (ti *tableIterator) SeekToLast() {
	numBlocks := len(ti.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		ti.err = io.EOF
		return
	}
	ti.blockPos = numBlocks - 1
	b, err := ti.t.block(ti.blockPos)
	if err != nil {
		ti.err = err
		return
	}
	ti.bi.tableID = ti.t.fid
	ti.bi.blockID = ti.blockPos
	ti.bi.setBlock(b)
	ti.bi.seekToLast()
	ti.it = ti.bi.Item()
	ti.err = ti.bi.Error()
}

func (ti *tableIterator) Rewind() {
	if ti.opt.IsAsc {
		ti.SeekToFirst()
	} else {
		ti.SeekToLast()
	}
}

func (it *tableIterator) Seek(key []byte) {
	var offset pb.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().Offsets), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&offset, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.t.ss.Indexs().Offsets) {
			return true
		}
		return utils.CompareKeys(offset.GetKey(), key) > 0
	})

	if idx == 0 {
		it.seekHelp(0, key)
		return
	}
	it.seekHelp(idx-1, key)
}

func (ti *tableIterator) seekHelp(idx int, key []byte) {
	b, err := ti.t.block(idx)
	if err != nil {
		ti.err = err
		return
	}
	ti.bi.tableID = ti.t.fid
	ti.bi.blockID = ti.blockPos
	ti.bi.setBlock(b)
	ti.bi.seek(key)
	ti.it = ti.bi.Item()
	ti.err = ti.bi.Error()
}

func decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	fmt.Println("test message")
	return nil
}
