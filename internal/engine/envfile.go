package engine

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

// ReadEnvFile reads a KEY=VALUE secrets file. It must be a regular file that
// group and others cannot access (chmod 600). Blank lines and lines starting
// with # are ignored; an optional "export " prefix and matching surrounding
// quotes are removed.
func ReadEnvFile(path string) (map[string]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("env_file: %v", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("env_file %s is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("env_file %s is accessible by group or others (mode %v); chmod 600 it", path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("env_file: %v", err)
	}
	out := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("env_file %s line %d: expected KEY=VALUE", path, n)
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		out[k] = v
	}
	return out, sc.Err()
}
