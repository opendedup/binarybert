package api

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
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
}

func NewKaggleChunker(outBase *string, min, max, avg *int) *KaggleChunker {
	maxSz, err := gounits.FromHumanSize("1GB")
	if err != nil {
		log.Error(err)
	}
	return &KaggleChunker{min: min, max: max, avg: avg, outBase: outBase, maxSize: maxSz}
}

func (n *KaggleChunker) ParseKaggle() error {
	for i := 1; i < 6; i++ {
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

			ck := NewChunker(f, int(*n.min), int(*n.avg), int(*n.max), of)
			err = ck.Chunk()
			if err != nil {
				log.Infof("Unable to create new chunker")
				return err
			}
			err = os.Remove(*tarpath)
			if err != nil {
				log.Fatal(err)
			}
			of.Close()
			f.Close()

		}
	}
	return nil
}

func (n *KaggleChunker) ParseKaggleNoTar() error {
	for i := 1; i < 6; i++ {
		repos, err := n.listKaggle(i, []string{})
		if err != nil {
			return err
		}
		for _, repo := range *repos {
			err := n.downloadKaggleNoTar(repo, *n.outBase)
			if err != nil {

				return err
			}
		}
	}
	repos, err := n.listKaggle(1, []string{"--search", "spotify"})
	if err != nil {
		return err
	}
	for _, repo := range *repos {
		err := n.downloadKaggleNoTar(repo, *n.outBase)
		if err != nil {

			return err
		}
	}
	repos, err = n.listKaggle(1, []string{"--search", "netflix"})
	if err != nil {
		return err
	}
	for _, repo := range *repos {
		err := n.downloadKaggleNoTar(repo, *n.outBase)
		if err != nil {

			return err
		}
	}
	return nil
}

func (n *KaggleChunker) downloadKaggleNoTar(repo, basfolder string) (err error) {
	pth := fmt.Sprintf("%s/%s", basfolder, strings.Replace(repo, "/", "_", -1))
	if strings.Contains(repo, "netflix") {
		fmt.Printf("############# %s\n", repo)
	}
	os.MkdirAll(pth, os.ModePerm)
	defer os.RemoveAll(pth)
	cmd := exec.Command("kaggle", "datasets", "download", "--unzip", "--path", pth, repo)

	err = cmd.Run()

	if err != nil {
		log.Infof("Skipping %s", repo)
		return err
	}
	fp := NewFileChunker(n.outBase, n.min, n.max, n.avg)
	fp.TBasePath = n.outBase
	if err != nil {
		log.Infof("196")
		return err
	}
	fldrs := []string{pth}
	err = fp.ParseFolder(&fldrs)
	if err != nil {
		log.Infof("296")
		return err
	}
	os.RemoveAll(pth)

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
