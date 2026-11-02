// Command stagehand is a small scheduled pipeline runner.
package main

import (
	"os"

	"github.com/gitmoot/stagehand/internal/cli"
)

func main() {
	os.Exit(cli.Main(os.Args[1:], os.Stdout, os.Stderr))
}
