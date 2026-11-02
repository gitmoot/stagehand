package shellexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const processPrefix = "process:"

// ProcessResource names a process group by leader pid and the leader's start
// time (in clock ticks since boot), so recovery never kills an unrelated
// group that reused the pid. Without /proc the start time is empty.
func ProcessResource(pgid int) string {
	return fmt.Sprintf("%s%d@%s", processPrefix, pgid, startTime(pgid))
}

// Cleanup kills a process group recorded by ProcessResource if its leader is
// still the same process. Other resources are ignored.
func (Executor) Cleanup(_ context.Context, resource string) error {
	rest, ok := strings.CutPrefix(resource, processPrefix)
	if !ok {
		return nil
	}
	pidText, start, _ := strings.Cut(rest, "@")
	pgid, err := strconv.Atoi(pidText)
	if err != nil || pgid <= 1 {
		return fmt.Errorf("bad process resource %q", resource)
	}
	// Kill only while the leader is the process we started: a reused pid has
	// a different start time. A group whose leader already exited is left
	// alone (it cannot be told apart from a new group with the same id); under
	// systemd the unit's cgroup is killed with the runner anyway.
	if start == "" || startTime(pgid) != start {
		return nil
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// startTime is field 22 of /proc/<pid>/stat, or "" if unavailable.
func startTime(pid int) string {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return ""
	}
	s := string(raw)
	// The command name (field 2) may contain spaces; fields resume after ')'.
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return ""
	}
	fields := strings.Fields(s[i+1:])
	// fields[0] is field 3 (state), so field 22 is fields[19].
	if len(fields) < 20 {
		return ""
	}
	return fields[19]
}
