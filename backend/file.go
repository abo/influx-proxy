// Copyright 2023 Shengbo Huang. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/util"
)

type FileBackend struct {
	lock     sync.Mutex
	filename string
	datadir  string
	dataflag bool
	producer *os.File
	consumer *os.File
	meta     *os.File
}

func NewFileBackend(filename string, datadir string) (fb *FileBackend, err error) {
	if err = util.MakeDir(datadir); err != nil {
		return
	}

	fb = &FileBackend{
		filename: filename,
		datadir:  datadir,
	}
	pathname := filepath.Join(datadir, filename)
	fb.producer, err = os.OpenFile(pathname+".dat", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("open producer error: %s %s", fb.filename, err)
		return
	}

	fb.consumer, err = os.OpenFile(pathname+".dat", os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("open consumer error: %s %s", fb.filename, err)
		return
	}

	fb.meta, err = os.OpenFile(pathname+".rec", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("open meta error: %s %s", fb.filename, err)
		return
	}

	fb.RollbackMeta()
	producerOffset, _ := fb.producer.Seek(0, io.SeekEnd)
	offset, _ := fb.consumer.Seek(0, io.SeekCurrent)
	fb.dataflag = producerOffset > offset
	return
}

func (fb *FileBackend) Write(p []byte) (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	var length = uint32(len(p))
	err = binary.Write(fb.producer, binary.BigEndian, length)
	if err != nil {
		log.Error("write length error: ", err)
		return
	}

	n, err := fb.producer.Write(p)
	if err != nil {
		log.Error("write error: ", err)
		return
	}
	if n != len(p) {
		return io.ErrShortWrite
	}

	err = fb.producer.Sync()
	if err != nil {
		log.Error("sync meta error: ", err)
		return
	}

	fb.dataflag = true
	return
}

func (fb *FileBackend) IsData() bool {
	fb.lock.Lock()
	defer fb.lock.Unlock()
	return fb.dataflag
}

func (fb *FileBackend) Read() (p []byte, err error) {
	if !fb.IsData() {
		return nil, nil
	}
	var length uint32

	err = binary.Read(fb.consumer, binary.BigEndian, &length)
	if err != nil {
		log.Error("read length error: ", err)
		return
	}
	p = make([]byte, length)

	_, err = io.ReadFull(fb.consumer, p)
	if err != nil {
		log.Error("read error: ", err)
		return
	}
	return
}

func (fb *FileBackend) RollbackMeta() (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	_, err = fb.meta.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("seek meta error: %s %s", fb.filename, err)
		return
	}

	var offset int64
	err = binary.Read(fb.meta, binary.BigEndian, &offset)
	if err != nil {
		if err != io.EOF {
			log.Errorf("read meta error: %s %s", fb.filename, err)
		}
		return
	}

	_, err = fb.consumer.Seek(offset, io.SeekStart)
	if err != nil {
		log.Errorf("seek consumer error: %s %s", fb.filename, err)
		return
	}
	return
}

func (fb *FileBackend) UpdateMeta() (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	producerOffset, err := fb.producer.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("seek producer error: %s %s", fb.filename, err)
		return
	}

	offset, err := fb.consumer.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("seek consumer error: %s %s", fb.filename, err)
		return
	}

	if producerOffset == offset {
		err = fb.CleanUp()
		if err != nil {
			log.Errorf("cleanup error: %s %s", fb.filename, err)
			return
		}
		offset = 0
	}

	_, err = fb.meta.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("seek meta error: %s %s", fb.filename, err)
		return
	}

	log.Errorf("write meta: %s, %d", fb.filename, offset)
	err = binary.Write(fb.meta, binary.BigEndian, &offset)
	if err != nil {
		log.Errorf("write meta error: %s %s", fb.filename, err)
		return
	}

	err = fb.meta.Sync()
	if err != nil {
		log.Errorf("sync meta error: %s %s", fb.filename, err)
		return
	}

	return
}

func (fb *FileBackend) CleanUp() (err error) {
	_, err = fb.consumer.Seek(0, io.SeekStart)
	if err != nil {
		log.Error("seek consumer error: ", err)
		return
	}
	filename := filepath.Join(fb.datadir, fb.filename+".dat")
	err = os.Truncate(filename, 0)
	if err != nil {
		log.Error("truncate error: ", err)
		return
	}
	err = fb.producer.Close()
	if err != nil {
		log.Error("close producer error: ", err)
		return
	}
	fb.producer, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Error("open producer error: ", err)
		return
	}
	fb.dataflag = false
	return
}

func (fb *FileBackend) Close() {
	fb.producer.Close()
	fb.consumer.Close()
	fb.meta.Close()
}
