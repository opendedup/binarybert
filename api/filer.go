package api

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/ahmetask/worker"
)

type FileChunker struct {
	min       *int
	max       *int
	avg       *int
	outBase   *string
	pool      *worker.Pool
	wg        sync.WaitGroup
	TBasePath *string
	Name      *string
	bufSize   *int
}

func NewFileChunker(outBase *string, min, max, avg, bufSize *int) *FileChunker {
	var pool = worker.NewWorkerPool(runtime.NumCPU(), 1)
	return &FileChunker{min: min, max: max, avg: avg, outBase: outBase, pool: pool, bufSize: bufSize}
}

func (n *FileChunker) ParseFile(inFile *string) error {
	_, file := path.Split(*inFile)
	if _, err := os.Stat(*inFile); errors.Is(err, os.ErrNotExist) {
		log.Infof("1")
		return err
	}
	fn := fmt.Sprintf("%s/%s.txt", *n.outBase, file)
	if _, err := os.Stat(fn); errors.Is(err, os.ErrNotExist) {

		of, err := os.Create(fn)
		if err != nil {
			log.Infof("2")
			return err
		}
		defer of.Close()
		// Open input file
		f, err := os.Open(*inFile)
		if err != nil {
			log.Infof("3")
			return err
		}
		defer f.Close()

		ck := NewChunker(f, int(*n.min), int(*n.avg), int(*n.max), *n.bufSize, of)
		err = ck.Chunk()
		if err != nil {
			log.Infof("4")
			return err
		}

	}
	return nil
}

func (n *FileChunker) ParseFolder(inFolders *[]string) error {
	n.pool.Start()
	for _, inFolder := range *inFolders {
		folder, err := filepath.Abs(inFolder)
		if err != nil {
			return err
		}
		err = filepath.Walk(folder, n.visitPath)
		n.wg.Wait()
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *FileChunker) visitPath(path string, f os.FileInfo, err error) error {
	fileInfo, _ := os.Lstat(path)
	if !fileInfo.IsDir() && fileInfo.Mode().IsRegular() && fileInfo.Size() > 0 {
		n.wg.Add(1)
		n.pool.Submit(&Job{file: &path, filer: n})
	}
	return nil
}

type Job struct {
	file *string

	filer *FileChunker
}

func (j *Job) Do() {
	if _, err := os.Stat(*j.file); errors.Is(err, os.ErrNotExist) {

		log.Infof("11")
		log.Error(err)
		return
	}
	defer j.filer.wg.Done()
	np := *j.file
	if j.filer.TBasePath != nil && len(*j.filer.TBasePath) > 0 {
		np = strings.Replace(np, *j.filer.TBasePath, "", 1)
	}
	if j.filer.Name != nil && len(*j.filer.Name) > 0 {
		np = *j.filer.Name + "#" + np
	}
	np = strings.Replace(np, "/", "#", -1)
	np = strings.Replace(np, "\\", "#", -1)
	np = strings.Replace(np, ":", "", -1)
	if strings.HasPrefix(np, "#") {
		np = strings.Replace(np, "#", "", 1)
	}
	np = strings.Replace(np, "##", "#", -1)

	fn := fmt.Sprintf("%s/%s.txt", *j.filer.outBase, np)
	if _, err := os.Stat(fn); errors.Is(err, os.ErrNotExist) {
		of, err := os.Create(fn)
		if err != nil {
			of.Close()
			log.Infof("22")
			log.Error(err)
			os.Remove(fn)
			return
		}
		defer of.Close()
		// Open input file
		f, err := os.Open(*j.file)
		if err != nil {
			log.Infof("33")
			log.Error(err)
			os.Remove(fn)
			return
		}
		defer f.Close()

		ck := NewChunker(f, int(*j.filer.min), int(*j.filer.avg), int(*j.filer.max), *j.filer.bufSize, of)
		err = ck.Chunk()
		if err != nil {
			of.Close()
			log.Infof("44")
			log.Error(err)
			os.Remove(fn)
			return
		}

	}
}
