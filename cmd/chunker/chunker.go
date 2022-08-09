package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/opendedup/binarybert/api"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] in-file\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Divide in-file into variable-sized, content-defined chunks that are robust to\n")
		fmt.Fprintf(os.Stderr, "insertions, deletions, and changes to in-file.\n\n")
		flag.PrintDefaults()
		os.Exit(2)
	}
	window := FlagBytes("window", 64, "use a rolling hash with window size `w`")
	avg := FlagBytes("avg", 4<<10, "average chunk `size`; must be a power of 2")
	min := FlagBytes("min", 512, "minimum chunk `size`")
	max := FlagBytes("max", 32<<10, "maximum chunk `size`")
	outBase := flag.String("out", "", "output folder")
	inFile := flag.String("infile", "", "input file")
	docker := flag.Bool("dockerimages", false, "parse docker images")
	images := [...]string{"ubuntu", "debian", "fedora", "centos", "tensorflow/tensorflow", "apache/spark", "alpine"}
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
			log.Fatal(err)
		}
		fn := fmt.Sprintf("%s/%s.txt", *outBase, file)
		if _, err := os.Stat(fn); errors.Is(err, os.ErrNotExist) {

			of, err := os.Create(fn)
			if err != nil {
				log.Fatal(err)
			}
			defer of.Close()
			// Open input file
			f, err := os.Open(*inFile)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			// Chunk and write output files.
			cpy := new(bytes.Buffer)
			r := io.TeeReader(f, cpy)
			ck := api.NewChunker(r, int(*min), int(*avg), int(*max), of)
			err = ck.Chunk()
			if err != nil {
				log.Fatal(err)
			}

		}

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
				log.Fatal(err)
			}
			err = json.Unmarshal(body, &dimages)
			if err != nil {
				log.Fatal(err)
			}
			for _, tag := range dimages {
				nm := tag.Name
				if len(nm) == 0 {
					nm = "latest"
				}
				ft := fmt.Sprintf("%s/%s.%s.tar", *outBase, element, nm)
				if _, err := os.Stat(fmt.Sprintf("%s.txt", ft)); errors.Is(err, os.ErrNotExist) {

					cmd := exec.Command("docker", "pull", fmt.Sprintf("%s:%s", element, nm))

					_, err := cmd.Output()
					if err != nil {
						log.Fatal(err)
					}

					cmd = exec.Command("docker", "save", "-o", ft, fmt.Sprintf("%s:%s", element, nm))

					_, err = cmd.Output()
					if err != nil {
						log.Fatal(err)
					}
					of, err := os.Create(fmt.Sprintf("%s.txt", ft))
					if err != nil {
						log.Fatal(err)
					}
					defer of.Close()
					// Open input file
					f, err := os.Open(ft)
					if err != nil {
						log.Fatal(err)
					}
					defer f.Close()

					// Chunk and write output files.
					cpy := new(bytes.Buffer)
					r := io.TeeReader(f, cpy)
					ck := api.NewChunker(r, int(*min), int(*avg), int(*max), of)
					err = ck.Chunk()
					if err != nil {
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
