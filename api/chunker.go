package api

import (
	"bytes"
	"hash/fnv"
	"io"
	"strings"

	b32 "encoding/base32"

	"github.com/aclements/go-rabin/rabin"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

type Chunker struct {
	rabinTable *rabin.Table
	r          io.Reader
	min        int
	max        int
	avg        int
	out        io.Writer
}

func NewChunker(r io.Reader, min, max, avg int, out io.Writer) *Chunker {
	return &Chunker{r: r, min: min, max: max, avg: avg, out: out, rabinTable: rabin.NewTable(rabin.Poly64, 64)}

}

func (n *Chunker) Chunk() error {
	h := fnv.New32a()
	b1 := make([]byte, 2048*1024)
	n1, err := n.r.Read(b1)
	if err != nil {
		log.Fatalf("error reading %s", err)
	}
	for n1 > 0 {
		s := make([]byte, n1)
		copy(s, b1)
		buf0 := bytes.NewBuffer(s)
		m := rabin.NewChunker(n.rabinTable, buf0, 4096, 8*1024, 128*1024)
		buf0_0 := bytes.NewBuffer(s)
		for z := 0; ; z++ {
			clen, err := m.Next()
			if err == io.EOF || clen == 0 {
				n.out.Write([]byte("\n"))
				break
			} else if err != nil {
				log.Fatal(err)
			}

			if err != nil {
				log.Fatal(err)
			}
			cbuf := new(bytes.Buffer)
			_, err = io.CopyN(cbuf, buf0_0, int64(clen))
			if err != nil {
				log.Errorf("error reading %s ", err)
				return err
			}
			buf2 := bytes.NewBuffer(cbuf.Bytes())
			c := rabin.NewChunker(n.rabinTable, buf2, n.min, n.avg, n.max)
			for i := 0; ; i++ {
				blen, err := c.Next()
				if err == io.EOF || blen == 0 {
					n.out.Write([]byte(" "))
					break
				} else if err != nil {
					log.Errorf("error writing out %v", err)
					return err
				}

				buf := new(bytes.Buffer)
				_, err = io.CopyN(buf, cbuf, int64(blen))
				if err != nil {
					log.Errorf("error writing %s", err)
					return err
				}
				h.Write(buf.Bytes())
				sEnc := b32.StdEncoding.EncodeToString(i32tob(h.Sum32()))
				n.out.Write([]byte(strings.ToLower(strings.Replace(sEnc, "=", "", -1))))
				h.Reset()

			}
		}
		n1, err = n.r.Read(b1)
		if n1 == 0 {
			break
		}
		if err != nil {
			log.Errorf("error reading %s %d", err, n1)
			return err
		}
	}
	return nil
}

func i32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}
