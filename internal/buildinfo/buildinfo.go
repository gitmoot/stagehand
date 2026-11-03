// Package buildinfo holds version data set at build time with -ldflags -X.
package buildinfo

import "fmt"

var (
	Version = "dev"
	Commit  = ""
	Date    = ""
)

// String is the one-line version shown by `stagehand version`.
func String() string {
	s := "stagehand " + Version
	if Commit != "" {
		s += fmt.Sprintf(" (%s", Commit)
		if Date != "" {
			s += ", " + Date
		}
		s += ")"
	}
	return s
}
