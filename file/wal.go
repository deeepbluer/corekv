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

package file

import (
	"bufio"
	"bytes"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/hardcore-os/corekv/utils"
)

// WalFile _
type WalFile struct {
	lock    *sync.RWMutex
	f       *MmapFile
	opts    *Options
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
}

// Fid _
func (wf *WalFile) Fid() uint64 {
	return wf.opts.FID
}

// Close _
func (wf *WalFile) Close() error {
	if err := wf.f.Close(); err != nil {
		return err
	}
	return os.Remove(wf.f.Fd.Name())
}

// Name _
func (wf *WalFile) Name() string {
	return wf.f.Fd.Name()
}

// Size 当前已经被写入的数据
func (wf *WalFile) Size() uint32 {
	return wf.writeAt
}

// OpenWalFile _
func OpenWalFile(opt *Options) *WalFile {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	wf := &WalFile{f: omf, lock: &sync.RWMutex{}, opts: opt}
	wf.buf = &bytes.Buffer{}
	wf.size = uint32(len(wf.f.Data))
	utils.Err(err)
	return wf
}

func (wf *WalFile) Write(entry *utils.Entry) error {
	wf.lock.Lock()
	defer wf.lock.Unlock()

	sz := utils.WalCodec(wf.buf, entry)
	utils.Panic(wf.f.AppendBuffer(wf.writeAt, wf.buf.Bytes()[:]))
	wf.writeAt += uint32(sz)
	return nil
}

// Iterate 从磁盘中遍历wal，获得数据
func (wf *WalFile) Iterate(readOnly bool, offset uint32, fn utils.LogEntry) (uint32, error) {
	reader := bufio.NewReader(wf.f.NewReader(int(offset)))
	sRead := &SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: offset,
		LF:           wf,
	}
	validOffset := offset
	for {
		entry, err := sRead.MakeEntry(reader)

		switch {
		case err == io.EOF:
			break
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break
		case err != nil:
			return 0, nil
		case entry.IsZero():
			break
		}

		var valPtr *utils.ValuePtr
		size := uint32(int(entry.LogHeaderLen()) + len(entry.Key) + len(entry.Value) + crc32.Size)
		sRead.RecordOffset += size
		validOffset = sRead.RecordOffset
		if err = fn(entry, valPtr); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function")
		}
	}
	return validOffset, nil
}

// Truncate _
// TODO Truncate 函数
func (wf *WalFile) Truncate(end int64) error {
	if end <= 0 {
		return nil
	}
	fileInfo, err := wf.f.Fd.Stat()
	if err != nil {
		return err
	}
	if fileInfo.Size() == end {
		return nil
	}
	wf.size = uint32(end)
	return wf.f.Truncature(end)
}

// 封装kv分离的读操作
type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	LF           *WalFile
}

// MakeEntry _
func (r *SafeRead) MakeEntry(reader io.Reader) (*utils.Entry, error) {
	tee := utils.NewHashReader(reader)
	var h utils.WalHeader
	hlen, err := h.Decode(tee)
	if err != nil {
		return nil, err
	}
	if h.KeyLen > uint32(1<<16) { // Key length must be below uint16.
		return nil, utils.ErrTruncate
	}
	kl := int(h.KeyLen)
	if cap(r.K) < kl {
		r.K = make([]byte, 2*kl)
	}
	vl := int(h.ValueLen)
	if cap(r.V) < vl {
		r.V = make([]byte, 2*vl)
	}

	e := &utils.Entry{}
	e.Offset = r.RecordOffset
	e.Hlen = hlen
	buf := make([]byte, h.KeyLen+h.ValueLen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	e.Key = buf[:h.KeyLen]
	e.Value = buf[h.KeyLen:]
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		return nil, utils.ErrTruncate
	}
	e.ExpiresAt = h.ExpiresAt
	return e, nil
}
