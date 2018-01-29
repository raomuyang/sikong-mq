package base

import (
	"os"
	"encoding/json"
	"bufio"
	"io"
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

