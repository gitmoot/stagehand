// Package sandboxd is a minimal client for sandboxd's strict
// ("gitmoot-strict") profile: create a Firecracker VM, upload files, run a
// command and stream its output, and destroy the VM.
//
// Wire format follows sandboxd's docs/compatibility.md and Gitmoot's client
// (internal/execbackend/e2b) at 1b124f60.
package sandboxd

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// guestPort is envd's port inside the VM, used for header routing.
const guestPort = "49983"

const connectJSON = "application/connect+json"

// Client talks to one sandboxd instance.
type Client struct {
	// BaseURL serves both the control API and the guest API (sandboxd runs
	// with -gateway-host matching its host).
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

// Sandbox is a created VM.
type Sandbox struct {
	ID          string `json:"sandboxID"`
	AccessToken string `json:"envdAccessToken"`
	TemplateID  string `json:"templateID"`
}

// CreateRequest is a VM to create. Metadata must carry job_id (unique per
// VM), attempt (>= 1) and lifecycle_generation (>= 0).
type CreateRequest struct {
	TemplateID string
	Timeout    time.Duration
	Metadata   map[string]string
}

// Create starts a VM.
func (c *Client) Create(ctx context.Context, r CreateRequest) (Sandbox, error) {
	secs := int(r.Timeout / time.Second)
	if secs < 1 {
		secs = 1
	}
	body, _ := json.Marshal(map[string]any{
		"templateID": r.TemplateID, "timeout": secs, "autoPause": false, "secure": true, "metadata": r.Metadata,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/sandboxes", bytes.NewReader(body))
	if err != nil {
		return Sandbox{}, err
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http().Do(req)
	if err != nil {
		return Sandbox{}, fmt.Errorf("sandboxd create: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return Sandbox{}, fmt.Errorf("sandboxd create: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var sb Sandbox
	if err := json.Unmarshal(raw, &sb); err != nil || sb.ID == "" || sb.AccessToken == "" {
		return Sandbox{}, fmt.Errorf("sandboxd create: unexpected response %q", truncate(raw))
	}
	return sb, nil
}

// Destroy deletes a VM. Deleting one that is already gone succeeds.
func (c *Client) Destroy(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.BaseURL+"/sandboxes/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", c.APIKey)
	resp, err := c.http().Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd destroy %s: %w", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("sandboxd destroy %s: HTTP %d: %s", id, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}

func (c *Client) guest(req *http.Request, sb Sandbox) {
	req.Header.Set("X-Access-Token", sb.AccessToken)
	req.Header.Set("E2b-Sandbox-Id", sb.ID)
	req.Header.Set("E2b-Sandbox-Port", guestPort)
}

// Upload writes a file in the VM (path must be under /home/user). The parent
// directory must exist.
func (c *Client) Upload(ctx context.Context, sb Sandbox, path string, data []byte) error {
	q := url.Values{"path": {path}, "username": {"user"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/files?"+q.Encode(), bytes.NewReader(data))
	if err != nil {
		return err
	}
	c.guest(req, sb)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http().Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd upload %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("sandboxd upload %s: HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}

// Process is a command to run in the VM, as the guest user.
type Process struct {
	Cmd  string            `json:"cmd"`
	Args []string          `json:"args,omitempty"`
	Env  map[string]string `json:"envs,omitempty"`
	Dir  string            `json:"cwd,omitempty"`
}

// Exec runs a command and streams its output. It returns the exit code.
// Cancelling ctx makes sandboxd destroy the VM.
func (c *Client) Exec(ctx context.Context, sb Sandbox, p Process, stdout, stderr io.Writer) (int, error) {
	payload, err := json.Marshal(map[string]any{"process": p, "stdin": false})
	if err != nil {
		return 0, err
	}
	frame := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	copy(frame[5:], payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/process.Process/Start", bytes.NewReader(frame))
	if err != nil {
		return 0, err
	}
	c.guest(req, sb)
	req.ContentLength = int64(len(frame))
	req.Header.Set("Content-Type", connectJSON)
	req.Header.Set("Accept", connectJSON)
	req.Header.Set("Connect-Protocol-Version", "1")
	resp, err := c.http().Do(req)
	if err != nil {
		return 0, fmt.Errorf("sandboxd exec: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, fmt.Errorf("sandboxd exec: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return readStream(resp.Body, stdout, stderr)
}

type streamEvent struct {
	Event struct {
		Start *struct {
			PID int `json:"pid"`
		} `json:"start"`
		Data *struct {
			Stdout []byte `json:"stdout"`
			Stderr []byte `json:"stderr"`
		} `json:"data"`
		End *struct {
			ExitCode int    `json:"exitCode"`
			Exited   bool   `json:"exited"`
			Status   string `json:"status"`
			Error    string `json:"error"`
		} `json:"end"`
		Keepalive *struct{} `json:"keepalive"`
	} `json:"event"`
}

func readStream(r io.Reader, stdout, stderr io.Writer) (int, error) {
	var (
		exitCode int
		ended    bool
		header   [5]byte
	)
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return 0, errors.New("sandboxd exec: stream ended without a terminal frame")
			}
			return 0, fmt.Errorf("sandboxd exec: read stream: %w", err)
		}
		flag, n := header[0], binary.BigEndian.Uint32(header[1:])
		if n > 64<<20 {
			return 0, fmt.Errorf("sandboxd exec: frame of %d bytes", n)
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(r, data); err != nil {
			return 0, fmt.Errorf("sandboxd exec: read frame: %w", err)
		}
		if flag&1 != 0 {
			return 0, errors.New("sandboxd exec: compressed frames are not supported")
		}
		if flag&2 != 0 { // end of stream
			if d := strings.TrimSpace(string(data)); d != "" && d != "{}" {
				return 0, fmt.Errorf("sandboxd exec: %s", truncate(data))
			}
			if !ended {
				return 0, errors.New("sandboxd exec: stream ended without a process result")
			}
			return exitCode, nil
		}
		var ev streamEvent
		if err := json.Unmarshal(data, &ev); err != nil {
			return 0, fmt.Errorf("sandboxd exec: decode event: %w", err)
		}
		switch e := ev.Event; {
		case e.Data != nil:
			if len(e.Data.Stdout) > 0 {
				if _, err := stdout.Write(e.Data.Stdout); err != nil {
					return 0, err
				}
			}
			if len(e.Data.Stderr) > 0 && stderr != nil {
				_, _ = stderr.Write(e.Data.Stderr)
			}
		case e.End != nil:
			if !e.End.Exited {
				return 0, errors.New("sandboxd exec: process end not confirmed")
			}
			if e.End.Error != "" && e.End.ExitCode == 0 {
				return 0, fmt.Errorf("sandboxd exec: %s", e.End.Error)
			}
			ended, exitCode = true, e.End.ExitCode
		}
	}
}

func truncate(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}
