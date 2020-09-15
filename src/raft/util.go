package raft

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

const Debug = 0

var (
	NANO_TO_MILLI = int64(1000000)

	Black   = Color("\033[1;30m%s\033[0m")
	Red     = Color("\033[1;31m%s\033[0m")
	Blue    = Color("\033[0;31m%s\033[0m")
	Green   = Color("\033[1;32m%s\033[0m")
	Yellow  = Color("\033[1;33m%s\033[0m")
	Purple  = Color("\033[1;34m%s\033[0m")
	Magenta = Color("\033[1;35m%s\033[0m")
	Teal    = Color("\033[1;36m%s\033[0m")
	White   = Color("\033[1;37m%s\033[0m")
	fp      *os.File
	seq     int
)

func init() {

	fn := os.Getenv("LOG_FILE")
	if fn == "" {
		fn = "raft.log"
	}
	if Debug == 0 {
		fp, _ = os.OpenFile(fn, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	}
}

func Dlog(format string, a ...interface{}) (n int, err error) {
	s := fmt.Sprintf(format, a...)
	if Debug > 0 {
		log.Infof(s)
	} else {
		fmt.Fprintf(fp, s)
	}
	return
}

func MicroSecondNow() int64 {
	return time.Now().UnixNano() / NANO_TO_MILLI
}

func randTs(from, to int) int64 {
	return int64(rand.Intn(to-from) + from)
}

func Color(colorString string) func(...interface{}) string {
	sprint := func(args ...interface{}) string {
		return fmt.Sprintf(colorString,
			fmt.Sprint(args...))
	}
	return sprint
}

func copyEntries(s []Entry) []Entry {

	r := make([]Entry, len(s))
	for i, v := range s {
		r[i] = v
	}
	return r
}

func clearLog() {
	if Debug == 0 {
		fp.Truncate(int64(0))
		fp.Seek(int64(0), 0)
	}
}

func seqInt() int {
	ret := seq
	seq++
	return ret
}

func minInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}
