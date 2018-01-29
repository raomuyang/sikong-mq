package base

import (
	"os"
	"encoding/json"
	"bufio"
	"io"
	"time"
	"math/rand"
	"math"
)

func UnmarshalJsonFile(instance interface{}, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var bufArray []byte

	buf := make([]byte, 1024)
	reader := bufio.NewReader(file)

	for {
		read, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		bufArray = append(bufArray, buf[:read]...)
	}

	json.Unmarshal(bufArray, instance)
	return nil
}

func RandSeconds(max int) time.Duration {
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s)
	return time.Duration(r.Intn(max))
}

type Backoff struct {
	times int
	Limit int
}

func (backoff *Backoff) Increase() time.Duration {
	if backoff.times + 2 >= backoff.GetLimit() {
		backoff.times = backoff.GetLimit()
	} else {
		backoff.times += 2
	}

	e := math.Log(float64(backoff.times))
	t := e * float64(time.Minute)
	return time.Duration(t) + RandSeconds(30)
}

func (backoff *Backoff) GetLimit() int {
	if backoff.Limit == 0 {
		return 30
	}
	return backoff.Limit
}

func (backoff *Backoff) Reset() {
	backoff.times = 0
}