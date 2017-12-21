package process

import (
	"log"
	"os"
	"io/ioutil"
	"io"
)

const DefaultLogFile = "skmq.log"

const (
	TRAC = 0
	INFO = 1
	WARN = 2
	ERROR = 3
)

var (
	Trace  *log.Logger
	Info   *log.Logger
	Warn   *log.Logger
	Err    *log.Logger
	writer io.Writer
)

func init() {
	file, err := os.OpenFile(DefaultLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0655)
	if err != nil {
		log.Fatalln("Failed to open log file.")
	}

	writer = io.MultiWriter(file, os.Stdout)

	Trace = log.New(ioutil.Discard, "[TRAC] ", log.Ldate|log.Ltime|log.Lshortfile)
	Info = log.New(writer, "[INFO] ", log.Ldate|log.Ltime|log.Lshortfile)
	Warn = log.New(writer, "[WARN] ", log.Ldate|log.Ltime|log.Lshortfile)
	Err = log.New(io.MultiWriter(file, os.Stderr),  "[ERRO] ", log.Ldate|log.Ltime|log.Lshortfile)
}

func SetLevel(level int)  {
	switch level {
	case TRAC:
		Trace.SetOutput(writer)
		fallthrough
	case INFO:
		Info.SetOutput(writer)
		fallthrough
	case WARN:
		Warn.SetOutput(writer)
		fallthrough
	case ERROR:
		if level == INFO {
			Trace.SetOutput(ioutil.Discard)
		}
		if level == WARN {
			Info.SetOutput(ioutil.Discard)
		}
		if level == ERROR {
			Warn.SetOutput(ioutil.Discard)
		}
	}
}

func SetOutfile(w io.Writer)  {
	writer = io.MultiWriter(w, os.Stdout)
	Info.SetOutput(writer)
	Warn.SetOutput(writer)

	Err.SetOutput(io.MultiWriter(w, os.Stderr))
}

