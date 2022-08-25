package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

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
	inFolder := flag.String("infolder", "", "input folder")
	docker := flag.Bool("dockerimages", false, "parse docker images")
	dockerFiles := flag.Bool("dockerFiles", false, "parse files in docker images")
	kaggleFiles := flag.Bool("kaggleFiles", false, "parse files in kaggle datasets")
	kaggle := flag.Bool("kaggle", false, "parse kaggle datasets")
	github := flag.Bool("github", false, "parse github projects")
	images := []string{"ubuntu",
		"debian", "fedora", "centos", "tensorflow/tensorflow", "apache/spark", "alpine",
		"busybox", "python", "postgres", "redis", "arm64v8/alpine", "arm64v8/ubuntu", "arm64v8/debian", "arm64v8/busybox",
		"arm64v8/postgres", "mongo", "arm64v8/mongo", "redis", "arm64v8/redis", "node", "arm64v8/node", "openjdk", "arm64v8/openjdk",
		"influxdb", "arm64v8/influxdb", "mysql", "arm64v8/mysql", "golang", "arm64v8/golang", "elasticsearch", "arm64v8/elasticsearch",
		"amazonlinux", "arm64v8/amazonlinux", "nextcloud", "arm64v8/nextcloud", "rabbitmq", "arm64v8/rabbitmq", "tomcat", "arm64v8/tomcat",
		"cassandra", "arm64v8/cassandra", "haproxy", "arm64v8/haproxy", "solr", "arm64v8/solr"}
	if runtime.GOOS == "windows" {
		fmt.Println("")
		images = images[:0]
		images = append(images, "windows/nanoserver",
			"windows/nanoserver/insider", "windows/servercore", "windows/server", "dotnet/sdk")
	}

	languages := []string{"java", "javascript", "python", "go", "c++", "c"}

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
	if len(*inFolder) > 0 {

		fcr := api.NewFileChunker(outBase, min, max, avg)
		fldrs := []string{*inFolder}
		err := fcr.ParseFolder(&fldrs)
		if err != nil {
			log.Fatal(err)
		}

	} else if len(*inFile) > 0 {
		fcr := api.NewFileChunker(outBase, min, max, avg)
		err := fcr.ParseFile(inFile)
		if err != nil {
			log.Fatal(err)
		}

	} else if *dockerFiles {
		bn := api.NewDockerChunker(outBase, min, max, avg)
		if runtime.GOOS != "windows" {
			err := bn.ParseDockerLinuxTar(images)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("DockerFiles not supported on windows")
		}

	} else if *docker {
		bn := api.NewDockerChunker(outBase, min, max, avg)
		if runtime.GOOS != "windows" {
			err := bn.ParseDockerLinux(images)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			err := bn.ParseDockerWindows(images)
			if err != nil {
				log.Fatal(err)
			}
		}

	} else if *github {
		gh := api.NewGitHubChunker(outBase, min, max, avg)
		err := gh.ParseGitHub(languages)
		if err != nil {
			log.Fatal(err)
		}

	} else if *kaggle {
		kg := api.NewKaggleChunker(outBase, min, max, avg)
		err := kg.ParseKaggle()
		if err != nil {
			log.Fatal(err)
		}
	} else if *kaggleFiles {
		bn := api.NewKaggleChunker(outBase, min, max, avg)
		err := bn.ParseKaggleNoTar()
		if err != nil {
			log.Fatal(err)
		}
	}

}
