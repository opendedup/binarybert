package api

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	gounits "github.com/docker/go-units"
)

type KaggleChunker struct {
	min     *int
	max     *int
	avg     *int
	outBase *string
	maxSize int64
	pqPath  *string
	bufSize *int
	files   int32
	pq      int32
	szp     int64
}

func NewKaggleChunker(outBase *string, min, max, avg, bufSize *int, pqPath *string) *KaggleChunker {
	maxSz, err := gounits.FromHumanSize("2GB")
	if err != nil {
		log.Error(err)
	}
	return &KaggleChunker{min: min, max: max, avg: avg, outBase: outBase, maxSize: maxSz, pqPath: pqPath, bufSize: bufSize}
}

func (n *KaggleChunker) ParseKaggle() error {
	for i := 1; i < 16; i++ {
		repos, err := n.listKaggle(i, []string{})
		if err != nil {
			return err
		}
		for _, repo := range *repos {
			tarpath, err := downloadKaggle(repo, *n.outBase)
			if err != nil {

				return err
			}
			of, err := os.Create(fmt.Sprintf("%s.txt", *tarpath))
			if err != nil {
				log.Infof("Unable to create output file")
				return err
			}

			// Open input file
			f, err := os.Open(*tarpath)
			if err != nil {
				log.Infof("Unable to create input file")
				return err
			}

			// Chunk and write output files.

			ck := NewChunker(f, int(*n.min), int(*n.avg), int(*n.max), *n.bufSize, of)
			err = ck.Chunk()
			if err != nil {
				log.Infof("Unable to create new chunker")
				return err
			}
			err = os.Remove(*tarpath)
			if err != nil {
				log.Errorf("unable to remove %s %v", *tarpath, err)
			}
			of.Close()
			f.Close()

		}
	}
	return nil
}

func (n *KaggleChunker) ParseKaggleNoTar() error {
	for i := 1; i < 8; i++ {
		repos, err := n.listKaggle(i, []string{})
		if err != nil {
			return err
		}
		for _, repo := range *repos {
			err := n.downloadKaggleNoTar(repo, *n.outBase)
			if err != nil {
				log.Infof("Error in repo %s, %v", repo, err)
				return err
			}
		}
	}
	repos, err := n.listKaggle(1, []string{"--search", "spotify"})
	if err != nil {
		log.Info("Error in listing spotify, %v", err)
		return err
	}
	for _, repo := range *repos {
		err := n.downloadKaggleNoTar(repo, *n.outBase)
		if err != nil {
			log.Infof("Error in spotify repo %s, %v", repo, err)
			return err
		}
	}
	repos, err = n.listKaggle(1, []string{"--search", "netflix"})
	if err != nil {
		log.Info("Error in listing netflix, %v", err)
		return err
	}
	for _, repo := range *repos {
		err := n.downloadKaggleNoTar(repo, *n.outBase)
		if err != nil {
			log.Infof("Error in netflix repo %s, %v", repo, err)
			return err
		}
	}
	fmt.Printf("Processed %d files and create %d pq files, read %d bytes\n", n.files, n.pq, n.szp)
	return nil
}

func (n *KaggleChunker) downloadKaggleNoTar(repo, basfolder string) (err error) {
	pth := fmt.Sprintf("%s/%s", basfolder, strings.Replace(repo, "/", "_", -1))
	os.MkdirAll(pth, os.ModePerm)
	defer os.RemoveAll(pth)
	cmd := exec.Command("kaggle", "datasets", "download", "--unzip", "--path", pth, repo)

	err = cmd.Run()

	if err != nil {
		log.Infof("Skipping %s because of download error", repo)
		return nil
	}
	fp := NewFileChunker(n.outBase, n.min, n.max, n.avg, n.bufSize)
	fp.TBasePath = n.outBase
	if err != nil {
		log.Infof("196")
		return err
	}
	fldrs := []string{pth}
	if n.pqPath != nil && len(*n.pqPath) > 0 {
		for _, inFolder := range fldrs {
			folder, err := filepath.Abs(inFolder)
			if err != nil {
				return err
			}
			filepath.Walk(folder, n.visitPath)
		}
	}
	err = fp.ParseFolder(&fldrs)
	if err != nil {
		log.Infof("296")
		return err
	}
	os.RemoveAll(pth)

	return nil

}

func (n *KaggleChunker) visitPath(path string, f os.FileInfo, err error) error {
	fileInfo, _ := os.Lstat(path)
	if !fileInfo.IsDir() && fileInfo.Mode().IsRegular() && fileInfo.Size() > 0 {
		fileExtension := filepath.Ext(path)

		if fileExtension == ".csv" || fileExtension == ".txt" {
			n.files++
			n.szp += fileInfo.Size()
			cmd := exec.Command("python", *n.pqPath, "-csv", path)

			err1 := cmd.Run()

			if err1 != nil {
				log.Warnf("Skipping %s", path)
			} else {
				npath := fmt.Sprintf("%s.parquet", path[:len(path)-4])
				fileInfo, _ = os.Lstat(npath)
				n.szp += fileInfo.Size()
				n.pq++
			}
		} else if fileExtension == ".json" {
			n.szp += fileInfo.Size()
			n.files++
			n.szp += fileInfo.Size()
			cmd := exec.Command("python", *n.pqPath, "-json", path)
			err1 := cmd.Run()

			if err1 != nil {
				log.Warnf("Skipping %s", path)
			} else {
				npath := fmt.Sprintf("%s.parquet", path[:len(path)-4])
				fileInfo, err = os.Lstat(npath)
				if err == nil {
					n.szp += fileInfo.Size()
					n.pq++
				}
			}
		} else {
			os.Remove(path)
		}
	}
	return nil
}

func downloadKaggle(repo, basfolder string) (tarfile *string, err error) {
	pth := fmt.Sprintf("%s/%s", basfolder, strings.Replace(repo, "/", "_", -1))
	os.MkdirAll(pth, os.ModePerm)
	defer os.RemoveAll(pth)
	cmd := exec.Command("kaggle", "datasets", "download", "--unzip", "--path", pth, repo)

	err = cmd.Run()

	if err != nil {
		log.Infof("Skipping %s", repo)
		return nil, err
	}
	tf := fmt.Sprintf("%s.tar", pth)
	cmd = exec.Command("tar", "-cf", tf, pth)
	_, err = cmd.Output()
	if err != nil {
		log.Infof("unable to tar %s from %s", tf, pth)
		return nil, err
	}

	return &tf, nil

}

func (n *KaggleChunker) listKaggle(page int, filter []string) (repos *[]string, err error) {
	cmdStr := []string{"datasets", "list", "--sort-by", "votes", "--csv", "--page", strconv.Itoa(page)}
	cmdStr = append(cmdStr, filter...)
	cmd := exec.Command("kaggle", cmdStr...)

	// Get a pipe to read from standard out
	r, _ := cmd.StdoutPipe()

	// Use the same pipe for standard error
	cmd.Stderr = cmd.Stdout

	// Make a new channel which will be used to ensure we get all output
	done := make(chan struct{})

	// Create a scanner which scans r in a line-by-line fashion
	scanner := bufio.NewScanner(r)

	// Use the scanner to scan the output line by line and log it
	// It's running in a goroutine so that it doesn't block
	var _repos []string
	go func() {

		// Read line by line and process it
		z := 0
		for scanner.Scan() {
			line := scanner.Text()
			if z != 0 {
				sz, err := gounits.FromHumanSize(strings.Split(line, ",")[2])
				if err != nil {
					log.Error(err)
				} else if sz < n.maxSize {
					_repos = append(_repos, strings.Split(line, ",")[0])
				} else {
					log.Infof("Skipped %s sz=%d maxSz=%d strsz=%s", strings.Split(line, ",")[0], sz, n.maxSize, strings.Split(line, ",")[2])
				}
			} else {
				fmt.Println(line)
			}
			z++
		}

		// We're all done, unblock the channel
		done <- struct{}{}

	}()

	// Start the command and check for errors
	err = cmd.Start()
	if err != nil {
		return repos, err
	}

	// Wait for all output to be processed
	<-done

	// Wait for the command to finish
	err = cmd.Wait()
	return &_repos, err
}
