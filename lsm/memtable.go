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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/pkg/errors"
)

const walFileExt string = ".wal"

// MemTable
type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *utils.Skiplist
	buf        *bytes.Buffer
	maxVersion uint64
}

// NewMemtable _
func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize), //TODO wal 要设置多大比较合理？ 姑且跟sst一样大
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
	}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkiplist(int64(1 << 20)), lsm: lsm}
}

// Close
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}

	return nil
}

func (m *memTable) set(entry *utils.Entry) error {
	// 写到wal 日志中，防止崩溃
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到memtable中
	m.sl.Add(entry)
	return nil
}

func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	// 索引检查当前的key是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	vs := m.sl.Search(key)

	e := &utils.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}

	return e, nil

}

func (m *memTable) Size() int64 {
	return m.sl.MemSize()
}

// recovery
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}

	var fids []uint64
	maxFid := lsm.levels.maxFID

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), walFileExt) {
			continue
		}
		fsz := len(file.Name())

		fid, err := strconv.ParseUint(file.Name()[:fsz-len(walFileExt)], 10, 64)
		if fid > maxFid {
			maxFid = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}

	sort.Slice(fids, func(i int, j int) bool {
		return fids[i] < fids[j]
	})

	var memTables []*memTable
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			// mt.DecrRef()
			continue
		}

		memTables = append(memTables, mt)
	}
	return lsm.NewMemtable(), memTables
}

func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: mtFilePath(lsm.option.WorkDir, fid),
	}
	s := utils.NewSkiplist(int64(1 << 20))
	mt := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = file.OpenWalFile(fileOpt)
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}
func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}

func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	endOff, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}
	// if endOff < m.wal.Size() {
	// 	return errors.WithMessage(utils.ErrTruncate, fmt.Sprintf("end offset: %d < size: %d", endOff, m.wal.Size()))
	// }
	return m.wal.Truncate(int64(endOff))
}

func (m *memTable) replayFunction(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error { // Function for replaying.
		if ts := utils.ParseTs(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.sl.Add(e)
		return nil
	}
}
