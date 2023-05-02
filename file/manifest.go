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
	"encoding/binary"
	"fmt"
	"github.com/hardcore-os/corekv/utils"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hardcore-os/corekv/pb"
)

// ManifestFile 维护sst文件元信息的文件
// manifest 比较特殊，不能使用mmap，需要保证实时的写入
type ManifestFile struct {
	opt                       *Options
	f                         *os.File
	lock                      sync.Mutex
	deletionsRewriteThreshold int
	manifest                  *Manifest
}

// Manifest corekv 元数据状态维护
type Manifest struct {
	Levels    []levelManifest
	Tables    map[uint64]TableManifest
	Creations int
	Deletions int
}

// TableManifest 包含sst的基本信息
type TableManifest struct {
	Level    uint8
	Checksum []byte // 方便今后扩展
}
type levelManifest struct {
	Tables map[uint64]struct{} // Set of table id's
}

// TableMeta sst 的一些元信息
type TableMeta struct {
	ID       uint64
	Checksum []byte
}

// OpenManifestFile 打开manifest文件
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{
		opt:  opt,
		lock: sync.Mutex{},
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		m := createManifest()
		fp, n, err := helpRewrite(opt.Dir, m)
		if err != nil {
			return nil, err
		}
		utils.CondPanic(n == 0, errors.Wrap(err, utils.ErrReWriteFailure.Error()))
		mf.manifest = m
		mf.f = fp
		mf.manifest.Creations = n
		return mf, nil
	}

	ret, truncOffset, err := ReplayManifestFile(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if err := file.Truncate(truncOffset); err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}
	mf.f = file
	mf.manifest = ret
	return mf, nil
}

// ReplayManifestFile 对已经存在的manifest文件重新应用所有状态变更
func ReplayManifestFile(fp *os.File) (ret *Manifest, truncOffset int64, err error) {
	r := &bufReader{reader: bufio.NewReader(fp)}
	magicBuf := make([]byte, 8)
	if _, err := io.ReadFull(r, magicBuf[:]); err != nil {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(magicBuf[0:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != utils.MagicVersion {
		return &Manifest{}, 0, fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, utils.MagicVersion)
	}

	build := createManifest()
	var offset int64
	for {
		offset = r.count
		lenCrc := make([]byte, 8)
		if _, err := io.ReadFull(r, lenCrc[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, fmt.Errorf("get lencrc error")
		}

		dataLen := binary.BigEndian.Uint32(lenCrc[:4])
		buf := make([]byte, dataLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, fmt.Errorf("get lencrc error")
		}

		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrc[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return &Manifest{}, 0, fmt.Errorf("unmarshal change set fail")
		}

		if err := applyChangeSet(build, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	}

	return build, offset, err
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
func applyChangeSet(build *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, mf := range changeSet.Changes {
		err := applyManifestChange(build, mf)
		if err != nil {
			return err
		}
	}
	return nil
}

func applyManifestChange(build *Manifest, tc *pb.ManifestChange) error {
	switch tc.Op {
	case pb.ManifestChange_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("sstable file %d has exist", tc.Id)
		}
		build.Tables[tc.Id] = TableManifest{
			Level:    uint8(tc.Level),
			Checksum: tc.Checksum,
		}
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, levelManifest{make(map[uint64]struct{})})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case pb.ManifestChange_DELETE:
		if _, ok := build.Tables[tc.Id]; !ok {
			return fmt.Errorf("delete change set fail, not found file %d", tc.Id)
		}
		delete(build.Tables, tc.Id)
		delete(build.Levels[tc.Level].Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("unknown operation of manifest change")
	}
	return nil
}

func createManifest() *Manifest {
	levels := make([]levelManifest, 0)
	return &Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

type bufReader struct {
	reader *bufio.Reader
	count  int64
}

func (r *bufReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.count += int64(n)
	return
}

// asChanges returns a sequence of changes that could be used to recreate the Manifest in its
// present state.
func (m *Manifest) asChanges() []*pb.ManifestChange {
	var changes []*pb.ManifestChange
	for fid, tableManifest := range m.Tables {
		changes = append(changes, &pb.ManifestChange{
			Id:       fid,
			Op:       pb.ManifestChange_CREATE,
			Level:    uint32(tableManifest.Level),
			Checksum: tableManifest.Checksum,
		})
	}
	return nil
}
func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

// Must be called while appendLock is held.
func (mf *ManifestFile) rewrite() error {
	if err := mf.f.Close(); err != nil {
		return err
	}

	rewritedFile, n, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}

	mf.f = rewritedFile
	mf.manifest.Creations = n
	mf.manifest.Deletions = 0
	return nil
}

func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	rewriteFile, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	copy(buf, utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion))

	netCreation := len(m.Tables)
	changes := m.asChanges()
	set := pb.ManifestChangeSet{Changes: changes}

	bytedSet, err := set.Marshal()
	if err != nil {
		rewriteFile.Close()
		return nil, 0, err
	}

	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[:4], uint32(len(bytedSet)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(bytedSet, utils.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, bytedSet...)

	if _, err := rewriteFile.Write(buf); err != nil {
		rewriteFile.Close()
		return nil, 0, err
	}
	if err := rewriteFile.Sync(); err != nil {
		rewriteFile.Close()
		return nil, 0, err
	}

	mfPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(rewritePath, mfPath); err != nil {
		rewriteFile.Close()
		return nil, 0, fmt.Errorf("rename manifest file fail")
	}

	mf, err := os.OpenFile(mfPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		rewriteFile.Close()
		return nil, 0, err
	}
	if _, err := mf.Seek(0, io.SeekEnd); err != nil {
		rewriteFile.Close()
		return nil, 0, fmt.Errorf("manifest fiel seek to end meet error")
	}
	if err := utils.SyncDir(dir); err != nil {
		rewriteFile.Close()
		return nil, 0, fmt.Errorf("sync dir meet error")
	}

	return rewriteFile, netCreation, nil
}

// Close 关闭文件
func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// AddChanges 对外暴露的写比那更丰富
func (mf *ManifestFile) AddChanges(changesParam []*pb.ManifestChange) error {
	return mf.addChanges(changesParam)
}
func (mf *ManifestFile) addChanges(changesParam []*pb.ManifestChange) error {
	changeSet := &pb.ManifestChangeSet{
		Changes: changesParam,
	}

	mf.lock.Lock()
	defer mf.lock.Unlock()

	err := applyChangeSet(mf.manifest, changeSet)
	if err != nil {
		return err
	}

	if mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		bytedChangeSet, err := changeSet.Marshal()
		if err != nil {
			return err
		}
		lenCrcBuf := make([]byte, 8)
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(bytedChangeSet)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(bytedChangeSet, utils.CastagnoliCrcTable))
		buf := append(lenCrcBuf, bytedChangeSet...)
		if _, err := mf.f.Write(buf); err != nil {
			return err
		}
	}
	err = mf.f.Sync()
	return err
}

// AddTableMeta 存储level表到manifest的level中
func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	mf.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID, levelNum, t.Checksum),
	})
	return err
}

// RevertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest.  idMap is a set of table file id's that were read from the directory
// listing.
func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	for id, _ := range mf.manifest.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	for id, _ := range idMap {
		if _, ok := mf.manifest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("Table file %d  not referenced in MANIFEST", id))
			fileName := utils.FileNameSSTable(mf.opt.Dir, id)
			if err := os.Remove(fileName); err != nil {
				return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

// GetManifest manifest
func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}
