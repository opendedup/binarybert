package api

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type DockerChunker struct {
	min     *int
	max     *int
	avg     *int
	outBase *string
}

func NewDockerChunker(outBase *string, min, max, avg *int) *DockerChunker {
	return &DockerChunker{min: min, max: max, avg: avg, outBase: outBase}
}

func (n *DockerChunker) ParseDockerLinuxTar(images []string) error {
	for _, element := range images {
		var myClient = &http.Client{Timeout: 10 * time.Second}
		hr, err := myClient.Get(fmt.Sprintf("https://registry.hub.docker.com/v1/repositories/%s/tags", element))
		if err != nil {

			return err
		}
		type Image struct {
			Name string `json:"name"`
		}
		type Manifest struct {
			Conifg   string   `json:"config"`
			RepoTags []string `json:"RepoTags"`
			Layers   []string `json:"Layers"`
		}
		var dimages []Image
		defer hr.Body.Close()
		body, err := ioutil.ReadAll(hr.Body)
		if err != nil {
			log.Infof("6")
			return err
		}
		err = json.Unmarshal(body, &dimages)
		if err != nil {
			log.Infof("7")
			return err
		} else {
			cr := strings.Replace(element, "/", "_", -1)
			fp := fmt.Sprintf("%s/%s", *n.outBase, cr)
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
				fldr := fmt.Sprintf("%s/%s.%s", fp, strings.Replace(element, "/", "_", -1), nm)

				if _, err := os.Stat(fldr); errors.Is(err, os.ErrNotExist) {
					os.MkdirAll(fldr, 0700)
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

							return err
						}
						tarFile, err := os.Open(ft)
						if err != nil {
							log.Infof("Unable to open %s", ft)

							return err
						}
						Untar(fldr, tarFile)
						mf, err := os.Open(fmt.Sprintf("%s/manifest.json", fldr))
						if err != nil {
							log.Infof("Unable to open %s", fmt.Sprintf("%s/manifest.json", fldr))

							return err
						}
						body, err := ioutil.ReadAll(mf)
						if err != nil {
							log.Infof("66")
							return err
						}
						var dmanifest []Manifest
						err = json.Unmarshal(body, &dmanifest)
						if err != nil {
							log.Infof("76")
							return err
						}
						dfldr := path.Join(fldr, "files")
						err = os.MkdirAll(dfldr, 0700)
						if err != nil {
							log.Infof("76")
							return err
						}
						for _, manifest := range dmanifest {
							for _, layer := range manifest.Layers {

								ltar := path.Join(fldr, layer)
								if _, err := os.Stat(ltar); err == nil {
									ltaro, err := os.Open(ltar)
									Untar(dfldr, ltaro)
									if err != nil {
										log.Infof("96")
										return err
									}
								} else {

								}

							}
						}
						dnm := fmt.Sprintf("%s#%s", strings.Replace(element, "/", "#", -1), nm)
						fp := NewFileChunker(n.outBase, n.min, n.max, n.avg)
						fp.Name = &dnm
						fp.TBasePath = &dfldr
						var fldrs []string
						fileInfo, err := ioutil.ReadDir(dfldr)
						if err != nil {
							log.Infof("196")
							return err
						}

						for _, file := range fileInfo {
							if file.IsDir() {
								fldrs = append(fldrs, path.Join(dfldr, file.Name()))
							}
						}
						err = fp.ParseFolder(&fldrs)
						if err != nil {
							log.Infof("296")
							return err
						}
						os.Remove(ft)
						os.RemoveAll(fldr)
						cmd = exec.Command("docker", "image", "rm", fmt.Sprintf("%s:%s", element, nm))
						cmd.Output()
					}
				}
			}
		}

	}
	return nil
}

func (n *DockerChunker) ParseDockerLinux(images []string) error {
	for _, element := range images {
		var myClient = &http.Client{Timeout: 10 * time.Second}
		hr, err := myClient.Get(fmt.Sprintf("https://registry.hub.docker.com/v1/repositories/%s/tags", element))
		if err != nil {

			return err
		}
		type Image struct {
			Name string `json:"name"`
		}
		var dimages []Image
		defer hr.Body.Close()
		body, err := ioutil.ReadAll(hr.Body)
		if err != nil {
			log.Infof("6")
			return err
		}
		err = json.Unmarshal(body, &dimages)
		if err != nil {
			log.Infof("7")
			return err
		} else {
			cr := strings.Replace(element, "/", "_", -1)
			fp := fmt.Sprintf("%s/%s", *n.outBase, cr)
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

							return err
						}
						of, err := os.Create(fmt.Sprintf("%s.txt", ft))
						if err != nil {
							log.Infof("Unable to create output file")
							return err
						}
						defer of.Close()
						// Open input file
						f, err := os.Open(ft)
						if err != nil {
							log.Infof("Unable to create input file")
							return err
						}
						defer f.Close()

						// Chunk and write output files.
						ck := NewChunker(f, int(*n.min), int(*n.avg), int(*n.max), of)
						err = ck.Chunk()
						if err != nil {
							log.Infof("Unable to create new chunker")
							return err
						}
						os.Remove(ft)
						cmd = exec.Command("docker", "image", "rm", fmt.Sprintf("%s:%s", element, nm))
						cmd.Output()
					}
				}
			}
		}

	}
	return nil
}

func (n *DockerChunker) ParseDockerWindows(images []string) error {
	var iq []string
	for _, element := range images {

		var myClient = &http.Client{Timeout: 10 * time.Second}
		hr, err := myClient.Get(fmt.Sprintf("https://mcr.microsoft.com/v2/%s/tags/list", element))
		if err != nil {

			log.Fatal(err)
		}
		type Tags struct {
			Name string   `json:"name"`
			Tags []string `json:"tags"`
		}
		var dimages Tags
		defer hr.Body.Close()
		body, err := ioutil.ReadAll(hr.Body)
		if err != nil {
			log.Infof("6")
			log.Fatal(err)
		}

		err = json.Unmarshal(body, &dimages)
		if err != nil {
			log.Infof("7 %s %s", body, fmt.Sprintf("https://mcr.microsoft.com/v2/%s/tags/list", element))
			log.Warn(err)
		} else {
			cr := strings.Replace(element, "/", "_", -1)
			fp := fmt.Sprintf("%s/%s", *n.outBase, cr)
			os.MkdirAll(fp, os.ModePerm)
			fmt.Printf("downloading %s to %s\n", element, fp)
			z := 0
			for _, tag := range dimages.Tags {
				if !strings.Contains(tag, "_") {
					z++
					if z == 100 {
						break
					}

					ft := fmt.Sprintf("%s/%s.%s.tar", fp, strings.Replace(element, "/", "_", -1), tag)
					if _, err := os.Stat(fmt.Sprintf("%s.txt", ft)); errors.Is(err, os.ErrNotExist) {

						cmd := exec.Command("docker", "pull", fmt.Sprintf("mcr.microsoft.com/%s:%s", element, tag))
						iq = append(iq, fmt.Sprintf("mcr.microsoft.com/%s:%s", element, tag))
						stdout, err := cmd.Output()
						if err != nil {
							log.Infof("Skipping %s:%s", element, tag)
							log.Warnf("Output %s", stdout)
							log.Warn(err)
						} else {

							cmd = exec.Command("docker", "save", "-o", ft, fmt.Sprintf("mcr.microsoft.com/%s:%s", element, tag))

							stdout, err = cmd.Output()
							if err != nil {
								log.Infof("Unable to save %s:%s", element, tag)
								log.Warnf("Output %s", stdout)
								return err
							}
							of, err := os.Create(fmt.Sprintf("%s.txt", ft))
							if err != nil {
								log.Infof("Unable to create output file")
								return err
							}
							defer of.Close()
							// Open input file
							f, err := os.Open(ft)
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
							f.Close()
							os.Remove(ft)
							if len(iq) >= 10 {
								var img string
								img, iq = iq[0], iq[1:]
								cmd = exec.Command("docker", "image", "rm", img)
								cmd.Output()
							}
						}
					}
				}
			}
		}

	}
	for _, img := range iq {
		cmd := exec.Command("docker", "image", "rm", img)
		cmd.Output()
	}
	return nil
}

func Untar(dst string, r io.Reader) error {

	tr := tar.NewReader(r)

	for {
		header, err := tr.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			return nil

		// return any other error
		case err != nil:
			return err

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}

		// the target location where the dir/file should be created
		target := filepath.Join(dst, header.Name)

		// the following switch could also be done using fi.Mode(), not sure if there
		// a benefit of using one vs. the other.
		// fi := header.FileInfo()

		// check the file type
		switch header.Typeflag {

		// if its a dir and it doesn't exist create it
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, 0755); err != nil {
					return err
				}
			}

		// if it's a file create it
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}

			// copy over contents
			if _, err := io.Copy(f, tr); err != nil {
				return err
			}

			// manually close here after each file operation; defering would cause each file close
			// to wait until all operations have completed.
			f.Close()
		}
	}
}
