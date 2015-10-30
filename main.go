package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	// "github.com/jordanorelli/moon"
	"io"
	"net/http"
	"strings"
)

var options struct {
	host string
	port int
}

type repl struct {
	in   io.Reader // reader of user input (stdin)
	out1 io.Writer // writer of evaluated queries (stdout)
	out2 io.Writer // writer of errors (stderr)
	host string
	port int

	br *bufio.Reader
}

func (r *repl) run() {
	r.br = bufio.NewReader(r.in)
	client := new(http.Client)
	var body bytes.Buffer
	for {
		// read url line
		line, err := r.br.ReadBytes('\n')
		if err != nil {
			r.errorf("error reading line: %v", err)
			continue
		}

		verb, url_b, err := splitUrlLine(line)
		if err != nil {
			r.errorf("bad url line: %v", err)
			continue
		}
		url := strings.TrimPrefix(string(url_b), "/")

		// read body
		if err := r.readBody(&body); err != nil {
			r.errorf("%v", err)
			continue
		}

		// compose http request
		fqurl := fmt.Sprintf("http://%s:%d/%s", r.host, r.port, url)
		req, err := http.NewRequest(verb, fqurl, &body)
		if err != nil {
			r.errorf("unable to create http request: %v", err)
			continue
		}

		res, err := client.Do(req)
		if err != nil {
			r.errorf("error sending http request: %v", err)
			continue
		}
		r.dumpResponse(res)
	}
}

func (r *repl) readBody(body *bytes.Buffer) error {
	body.Reset()
	for {
		line, err := r.br.ReadBytes('\n')
		if err != nil {
			return fmt.Errorf("error reading body: %v", err)
		}
		if len(bytes.TrimSpace(line)) == 0 {
			return nil
		}
		if _, err := body.Write(line); err != nil {
			return fmt.Errorf("error building body: %v", err)
		}
	}
}

func (r *repl) dumpResponse(res *http.Response) {
	defer res.Body.Close()

	var w io.Writer
	if res.StatusCode < 200 || res.StatusCode > 299 {
		w = r.out2
		fmt.Fprintf(w, "Status: %d\n", res.StatusCode)
	} else {
		w = r.out1
	}

	io.Copy(w, res.Body)
	w.Write([]byte("\n"))
}

func (r *repl) errorf(msg string, args ...interface{}) {
	if !strings.HasSuffix(msg, "\n") {
		msg += "\n"
	}
	fmt.Fprintf(r.out2, msg, args...)
}

func splitUrlLine(line []byte) (string, []byte, error) {
	parts := bytes.SplitN(line, []byte{' '}, 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("wrong number of url line parts. found %d, expected 2", len(parts))
	}
	verb_b, url_b := parts[0], parts[1]

	verb := strings.ToLower(string(verb_b))
	switch verb {
	case "get", "post", "put", "delete":
	default:
		return "", nil, fmt.Errorf("illegal verb: %s", verb)
	}

	return verb, bytes.TrimSpace(url_b), nil
}

func main() {
	flag.Parse()
	r := repl{
		in:   os.Stdin,
		out1: os.Stdout,
		out2: os.Stderr,
		host: options.host,
		port: options.port,
	}
	r.run()
}

func init() {
	flag.StringVar(&options.host, "host", "localhost", "ES host")
	flag.IntVar(&options.port, "port", 9200, "ES port")
}
