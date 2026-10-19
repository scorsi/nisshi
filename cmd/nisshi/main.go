// nisshi, the log store: validator (ADR 0004), server (ADR 0003), read and verify (ADR 0010,
// 0007).
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"nisshi/internal/logschema"
	"nisshi/internal/serve"
	"nisshi/internal/store"
)

const usage = `usage:
  nisshi validate --logs < lines                  (raw service output or journalctl -o cat)
  nisshi serve <config.toml>
  nisshi read <root> --node <n> [--service <s>] [--event <e>] --from <ts> [--to <ts>]
  nisshi verify <root>`

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fail := func(err error) int { fmt.Fprintf(stderr, "nisshi: %v\n", err); return 1 }
	switch {
	case len(args) == 2 && args[0] == "validate" && args[1] == "--logs":
		return validate(stdin, stderr)
	case len(args) == 2 && args[0] == "serve":
		cfg, err := serve.LoadConfig(args[1])
		if err != nil {
			return fail(err)
		}
		if err := serve.Run(cfg, serve.NewLogger(stderr)); err != nil {
			return fail(err)
		}
		return 0
	case len(args) == 2 && args[0] == "verify":
		if err := store.Verify(args[1], stderr); err != nil {
			return fail(err)
		}
		return 0
	case len(args) >= 2 && args[0] == "read":
		fs := flag.NewFlagSet("read", flag.ContinueOnError)
		fs.SetOutput(stderr)
		var q store.Query
		var from, to string
		fs.StringVar(&q.Node, "node", "", "node name (required)")
		fs.StringVar(&q.Service, "service", "", "service filter")
		fs.StringVar(&q.Event, "event", "", "event filter")
		fs.StringVar(&from, "from", "", "start, RFC 3339 or offset such as -1h (required)")
		fs.StringVar(&to, "to", "", "end, RFC 3339 or offset; default now")
		if fs.Parse(args[2:]) != nil || q.Node == "" || from == "" {
			fmt.Fprintln(stderr, usage)
			return 2
		}
		now := time.Now()
		var err error
		if q.From, err = store.ParseTime(from, now); err != nil {
			return fail(err)
		}
		q.To = now
		if to != "" {
			if q.To, err = store.ParseTime(to, now); err != nil {
				return fail(err)
			}
		}
		if err := store.Read(args[1], q, stdout); err != nil {
			return fail(err)
		}
		return 0
	}
	fmt.Fprintln(stderr, usage)
	return 2
}

func validate(stdin io.Reader, stderr io.Writer) int {
	sc := bufio.NewScanner(stdin)
	// One byte over the limit plus the newline: a longer line ends the scan with ErrTooLong.
	sc.Buffer(make([]byte, 64<<10), logschema.MaxLineBytes+2)
	n := 0
	for sc.Scan() {
		n++
		if reason := logschema.Line(sc.Bytes()); reason != "" {
			fmt.Fprintf(stderr, "line %d: %s\n", n, reason)
			return 1
		}
	}
	if err := sc.Err(); errors.Is(err, bufio.ErrTooLong) {
		fmt.Fprintf(stderr, "line %d: line-too-large\n", n+1)
		return 1
	} else if err != nil {
		fmt.Fprintf(stderr, "nisshi: reading stdin: %v\n", err)
		return 1
	}
	return 0
}
