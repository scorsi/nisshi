// nisshi, the log store. M0: the log validator only (ADR 0004).
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"nisshi/internal/logschema"
)

const usage = "usage: nisshi validate --logs < lines   (raw service output or journalctl -o cat)"

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stderr)) }

func run(args []string, stdin io.Reader, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "validate" || args[1] != "--logs" {
		fmt.Fprintln(stderr, usage)
		return 2
	}
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
