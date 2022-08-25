package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type GitHubChunker struct {
	min     *int
	max     *int
	avg     *int
	outBase *string
}

func NewGitHubChunker(outBase *string, min, max, avg *int) *GitHubChunker {
	return &GitHubChunker{min: min, max: max, avg: avg, outBase: outBase}
}

func (n *GitHubChunker) ParseGitHub(languages []string) error {
	var myClient = &http.Client{Timeout: 10 * time.Second}
	for _, language := range languages {
		turl := fmt.Sprintf("https://gh-trending-api.herokuapp.com/repositories/%s", language)
		hr, err := myClient.Get(turl)
		if err != nil {
			log.Fatal(err)
		}
		type Repo struct {
			Url string `json:"url"`
		}
		var repos []Repo
		defer hr.Body.Close()
		body, err := ioutil.ReadAll(hr.Body)
		if err != nil {
			log.Infof("6")
			log.Fatal(err)
		}
		err = json.Unmarshal(body, &repos)
		if err != nil {
			log.Infof("7")
			log.Warn(err)
		}
		basedir := fmt.Sprintf("%s/%s", *n.outBase, language)
		os.MkdirAll(basedir, os.ModePerm)
		for _, repo := range repos {
			reponame := filepath.Base(repo.Url)
			repopath := fmt.Sprintf("%s/%s", basedir, reponame)
			if _, err := os.Stat(fmt.Sprintf("%s.txt", repopath)); errors.Is(err, os.ErrNotExist) {
				cmd := exec.Command("git", "clone", repo.Url, repopath)

				_, err := cmd.Output()
				if err != nil {
					log.Infof("Skipping %s", filepath.Base(repo.Url))
					log.Warn(err)
				}
				tarpath := fmt.Sprintf("%s.tar", repopath)
				cmd = exec.Command("tar", "-cf", tarpath, repopath)
				_, err = cmd.Output()
				if err != nil {
					log.Infof("error taring %s", repopath)
					log.Fatal(err)
				}
				of, err := os.Create(fmt.Sprintf("%s.txt", tarpath))
				if err != nil {
					log.Infof("Unable to create output file")
					log.Fatal(err)
				}

				// Open input file
				f, err := os.Open(tarpath)
				if err != nil {
					log.Infof("Unable to create input file")
					log.Fatal(err)
				}

				// Chunk and write output files.
				ck := NewChunker(f, int(*n.min), int(*n.avg), int(*n.max), of)
				err = ck.Chunk()
				if err != nil {
					log.Infof("Unable to create new chunker")
					log.Fatal(err)
				}
				err = os.RemoveAll(repopath)
				if err != nil {
					log.Fatal(err)
				}
				err = os.Remove(tarpath)
				if err != nil {
					log.Fatal(err)
				}
				of.Close()
				f.Close()
			}

		}
	}
	return nil
}
