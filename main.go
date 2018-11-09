package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cupcake/rdb"
)

const delim = ":"
const maxChildren = 10

type redisKeyTree struct {
	root *redisKeyTreeNode
}

type redisKeyTreeNode struct {
	wildChild *redisKeyTreeNode
	children  map[string]*redisKeyTreeNode
	key       string
	count     int
	size      int
}

type rdbDecoder struct {
	rkt *redisKeyTree
}

func (d *rdbDecoder) StartRDB()                     {}
func (d *rdbDecoder) StartDatabase(int)             {}
func (d *rdbDecoder) Aux([]byte, []byte)            {}
func (d *rdbDecoder) ResizeDatabase(uint32, uint32) {}

func (d *rdbDecoder) Set(k []byte, v []byte, expire int64) {
	d.rkt.processKey(string(k))
	d.rkt.incrementSize(string(k), len(string(v)))
}

func (d *rdbDecoder) StartHash(k []byte, length, expiry int64) {
	d.rkt.processKey(string(k))
}

func (d *rdbDecoder) Hset(key, field, value []byte) {
	d.rkt.incrementSize(string(key), len(field)+len(value))
}

func (d *rdbDecoder) EndHash([]byte) {}

func (d *rdbDecoder) StartSet(k []byte, length, expiry int64) {
	d.rkt.processKey(string(k))
}

func (d *rdbDecoder) Sadd(key []byte, value []byte) {
	d.rkt.incrementSize(string(key), len(string(value)))
}

func (d *rdbDecoder) EndSet([]byte) {}
func (d *rdbDecoder) StartList(k []byte, length, expiry int64) {
	d.rkt.processKey(string(k))
}

func (d *rdbDecoder) Rpush(key, value []byte) {
	d.rkt.incrementSize(string(key), len(string(value)))
}

func (d *rdbDecoder) EndList(key []byte) {}

func (d *rdbDecoder) StartZSet(k []byte, cardinality, expiry int64) {
	d.rkt.processKey(string(k))
}

func (d *rdbDecoder) Zadd(key []byte, score float64, member []byte) {
	d.rkt.incrementSize(string(key), len(string(member)))
}

func (d *rdbDecoder) EndZSet(key []byte) {}
func (d *rdbDecoder) EndDatabase(n int)  {}
func (d *rdbDecoder) EndRDB()            {}

func newRedisKeyTreeNode(key string) *redisKeyTreeNode {
	return &redisKeyTreeNode{
		children: map[string]*redisKeyTreeNode{},
		count:    0,
		size:     0,
		key:      key,
	}
}

func (rkt *redisKeyTree) processKey(k string) error {
	toks := strings.Split(k, delim)
	if len(toks) == 0 {
		return errors.New("empty key")
	}

	return rkt.root.processKey(toks)
}

func (rkt *redisKeyTree) incrementSize(k string, n int) error {
	toks := strings.Split(k, delim)
	if len(toks) == 0 {
		return errors.New("empty key")
	}

	return rkt.root.incrementSize(toks, n)
}

func (rktn *redisKeyTreeNode) processKey(toks []string) error {
	rktn.count++

	if len(toks) == 0 {
		return nil
	}

	if rktn.wildChild == nil {
		rktn.wildChild = newRedisKeyTreeNode("*")
	}
	rktn.wildChild.processKey(toks[1:])

	child, ok := rktn.children[toks[0]]
	if !ok {
		if len(rktn.children) >= maxChildren {
			return nil
		}

		child = newRedisKeyTreeNode(toks[0])
		rktn.children[toks[0]] = child
	}
	child.processKey(toks[1:])

	return nil
}

func (rkt *redisKeyTreeNode) incrementSize(toks []string, n int) error {
	rkt.size += n

	if len(toks) == 0 {
		return nil
	}

	if rkt.wildChild != nil {
		rkt.wildChild.incrementSize(toks[1:], n)
	}

	child, ok := rkt.children[toks[0]]
	if !ok {
		return nil
	}

	return child.incrementSize(toks[1:], n)
}

func (rkt *redisKeyTree) print(w io.Writer) error {
	s := fmt.Sprintf("%-20s %-20s key\n", "count", "size")
	w.Write([]byte(s))
	rkt.root.print(w, "")
	return nil
}

func (rktn *redisKeyTreeNode) print(w io.Writer, prefix string) error {
	if rktn.wildChild == nil || rktn.key == "*" {
		s := fmt.Sprintf("%-20d %-20d %s%s\n", rktn.count, rktn.size, prefix, rktn.key)
		w.Write([]byte(s))
	}

	if rktn.wildChild != nil {
		rktn.wildChild.print(w, prefix+rktn.key+delim)
	}
	for k := range rktn.children {
		rktn.children[k].print(w, prefix+rktn.key+delim)
	}
	return nil
}

func main() {
	var err error

	rkt := &redisKeyTree{
		root: newRedisKeyTreeNode("ROOT"),
	}
	reader := bufio.NewReader(os.Stdin)

	decoder := &rdbDecoder{
		rkt: rkt,
	}

	err = rdb.Decode(reader, decoder)
	if err != nil {
		panic(err)
	}

	err = rkt.print(os.Stdout)
	if err != nil {
		panic(err)
	}

}
