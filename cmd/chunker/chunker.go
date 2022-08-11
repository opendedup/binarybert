package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/opendedup/binarybert/api"
	"github.com/sirupsen/logrus"

	"net/http"
	_ "net/http/pprof"
)

var log = logrus.New()

func main() {
	go func() {
		log.Println(http.ListenAndServe(":8081", nil))
	}()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] in-file\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Divide in-file into variable-sized, content-defined chunks that are robust to\n")
		fmt.Fprintf(os.Stderr, "insertions, deletions, and changes to in-file.\n\n")
		flag.PrintDefaults()
		os.Exit(2)
	}
	window := flag.Int("window", 48, "use a rolling hash with window size `w`")
	avg := flag.Int("avg", 2048, "average chunk `size`; must be a power of 2")
	min := flag.Int("min", 96, "minimum chunk `size`")
	max := flag.Int("max", 128*1024, "maximum chunk `size`")
	outBase := flag.String("out", "", "output folder")
	inFile := flag.String("infile", "", "input file")
	docker := flag.Bool("dockerimages", false, "parse docker images")
	kaggle := flag.Bool("kaggle", false, "parse kaggle datasets")
	github := flag.Bool("github", false, "parse github projects")
	images := [...]string{"ubuntu",
		"debian", "fedora", "centos", "tensorflow/tensorflow", "apache/spark", "alpine",
		"busybox", "python", "postgres", "redis", "arm64v8/alpine", "arm64v8/ubuntu", "arm64v8/debian", "arm64v8/busybox",
		"arm64v8/postgres"}
	languages := [...]string{"java", "javascript", "python", "go", "c++", "c"}
	var myClient = &http.Client{Timeout: 10 * time.Second}

	flag.Parse()
	if *min > *max {
		log.Fatal("-min must be <= -max")
	}
	if *avg&(*avg-1) != 0 {
		log.Fatal("-avg must be a power of two")
	}
	if *min < *window {
		log.Fatal("-min must be >= -window")
	}
	if len(*outBase) == 0 {
		log.Fatal("out must be set")
	}
	if len(*inFile) > 0 {
		_, file := path.Split(*inFile)
		if _, err := os.Stat(*inFile); errors.Is(err, os.ErrNotExist) {
			log.Infof("1")
			log.Fatal(err)
		}
		fn := fmt.Sprintf("%s/%s.txt", *outBase, file)
		if _, err := os.Stat(fn); errors.Is(err, os.ErrNotExist) {

			of, err := os.Create(fn)
			if err != nil {
				log.Infof("2")
				log.Fatal(err)
			}
			defer of.Close()
			// Open input file
			f, err := os.Open(*inFile)
			if err != nil {
				log.Infof("3")
				log.Fatal(err)
			}
			defer f.Close()

			ck := api.NewChunker(f, int(*min), int(*avg), int(*max), of)
			err = ck.Chunk()
			if err != nil {
				log.Infof("4")
				log.Fatal(err)
			}

		}
		return

	} else if *docker {
		for _, element := range images {

			hr, err := myClient.Get(fmt.Sprintf("https://registry.hub.docker.com/v1/repositories/%s/tags", element))
			if err != nil {

				log.Fatal(err)
			}
			type Image struct {
				Name string `json:"name"`
			}
			var dimages []Image
			defer hr.Body.Close()
			body, err := ioutil.ReadAll(hr.Body)
			if err != nil {
				log.Infof("6")
				log.Fatal(err)
			}
			err = json.Unmarshal(body, &dimages)
			if err != nil {
				log.Infof("7")
				log.Warn(err)
			} else {
				cr := strings.Replace(element, "/", "_", -1)
				fp := fmt.Sprintf("%s/%s", *outBase, cr)
				os.MkdirAll(fp, os.ModePerm)
				fmt.Printf("downloading %s to %s\n", element, fp)
				z := 0
				for _, tag := range dimages {
					z++
					if z == 100 {
						break
					}
					nm := tag.Name
					if len(nm) == 0 {
						nm = "latest"
					}
					ft := fmt.Sprintf("%s/%s.%s.tar", fp, strings.Replace(element, "/", "_", -1), nm)
					if _, err := os.Stat(fmt.Sprintf("%s.txt", ft)); errors.Is(err, os.ErrNotExist) {

						cmd := exec.Command("docker", "pull", fmt.Sprintf("%s:%s", element, nm))

						_, err := cmd.Output()
						if err != nil {
							log.Infof("Skipping %s:%s", element, nm)
							log.Warn(err)
						} else {

							cmd = exec.Command("docker", "save", "-o", ft, fmt.Sprintf("%s:%s", element, nm))

							_, err = cmd.Output()
							if err != nil {
								log.Infof("Unable to save %s:%s", element, nm)

								log.Fatal(err)
							}
							of, err := os.Create(fmt.Sprintf("%s.txt", ft))
							if err != nil {
								log.Infof("Unable to create output file")
								log.Fatal(err)
							}
							defer of.Close()
							// Open input file
							f, err := os.Open(ft)
							if err != nil {
								log.Infof("Unable to create input file")
								log.Fatal(err)
							}
							defer f.Close()

							// Chunk and write output files.
							cpy := new(bytes.Buffer)
							r := io.TeeReader(f, cpy)
							ck := api.NewChunker(r, int(*min), int(*avg), int(*max), of)
							err = ck.Chunk()
							if err != nil {
								log.Infof("Unable to create new chunker")
								log.Fatal(err)
							}
							os.Remove(ft)
							cmd = exec.Command("docker", "image", "rm", fmt.Sprintf("%s:%s", element, nm))
							cmd.Output()
						}
					}
				}
			}

		}
	} else if *github {
		fmt.Println("Getting github data")
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
			basedir := fmt.Sprintf("%s/%s", *outBase, language)
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
					cpy := new(bytes.Buffer)
					r := io.TeeReader(f, cpy)
					ck := api.NewChunker(r, int(*min), int(*avg), int(*max), of)
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

	} else if *kaggle {
		for i := 0; i < 5; i++ {
			repos, err := listKaggle(i)
			if err != nil {
				log.Fatal(err)
			}
			for _, repo := range *repos {
				tarpath, err := downloadKaggle(repo, *outBase)
				if err != nil {

					log.Fatal(err)
				}
				of, err := os.Create(fmt.Sprintf("%s.txt", *tarpath))
				if err != nil {
					log.Infof("Unable to create output file")
					log.Fatal(err)
				}

				// Open input file
				f, err := os.Open(*tarpath)
				if err != nil {
					log.Infof("Unable to create input file")
					log.Fatal(err)
				}

				// Chunk and write output files.
				cpy := new(bytes.Buffer)
				r := io.TeeReader(f, cpy)
				ck := api.NewChunker(r, int(*min), int(*avg), int(*max), of)
				err = ck.Chunk()
				if err != nil {
					log.Infof("Unable to create new chunker")
					log.Fatal(err)
				}
				err = os.Remove(*tarpath)
				if err != nil {
					log.Fatal(err)
				}
				of.Close()
				f.Close()

			}
		}
	}

}

func downloadKaggle(repo, basfolder string) (tarfile *string, err error) {
	pth := fmt.Sprintf("%s/%s", basfolder, strings.Replace(repo, "/", "_", -1))
	os.MkdirAll(pth, os.ModePerm)
	defer os.RemoveAll(pth)
	cmd := exec.Command("kaggle", "dataset", "download", "--unzip", "--path", pth)
	_, err = cmd.Output()
	if err != nil {
		log.Infof("Skipping %s", repo)
		return nil, err
	}
	tf := fmt.Sprintf("%s/%s.tar", basfolder, pth)
	cmd = exec.Command("tar", "-cf", tf, pth)
	_, err = cmd.Output()
	if err != nil {
		log.Infof("unable to tar %s", repo)
		return nil, err
	}

	return &tf, nil

}

func listKaggle(page int) (repos *[]string, err error) {
	cmd := exec.Command("kaggle", "datasets", "list", "--sort-by", "votes", "--csv", "--page", string(page))

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
		for scanner.Scan() {
			line := scanner.Text()
			_repos = append(_repos, strings.Split(line, ",")[0])
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
