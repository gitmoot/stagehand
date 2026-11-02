package agentexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/gitmoot/stagehand/internal/modelgw"
	"github.com/gitmoot/stagehand/internal/sandboxd"
)

const (
	guestHome     = "/home/user"
	guestWork     = guestHome + "/work"
	guestAgentDir = guestHome + "/.stagehand/agent"
	// sandboxPrefix marks a tracked resource as a sandboxd VM.
	sandboxPrefix = "sandboxd:"
)

// Sandboxd runs omp in a fresh sandboxd Firecracker VM per attempt. The VM
// gets the input files and nothing else from the host: no environment, no
// credentials. Its model calls go to a per-attempt run gateway that admits
// only that attempt's random token.
type Sandboxd struct {
	Client *sandboxd.Client
	// Template is the strict-profile template id or alias.
	Template string
	// GuestOmp is the omp binary inside the VM.
	GuestOmp string
	// MaxTTL is the instance's -max-ttl; VM lifetimes are capped to it.
	MaxTTL time.Duration
	// GatewayListen is where the run gateway listens on this host: the
	// loopback side of the instance's -fc-host-port.
	GatewayListen string
	// GuestGatewayURL is the same port as the guest sees it
	// (http://10.0.2.2:<port>).
	GuestGatewayURL string
	// GatewayUpstream is where model calls go next (the tunnel to the
	// credential proxy).
	GatewayUpstream string
	Log             *slog.Logger
}

// Run creates a VM, runs omp in it, and destroys it.
func (s Sandboxd) Run(ctx context.Context, req Request, onLine func([]byte)) (int, error) {
	token, err := modelgw.NewToken()
	if err != nil {
		return 0, err
	}
	gw, err := modelgw.StartRunGateway(s.GatewayListen, s.GatewayUpstream, token, s.Log)
	if err != nil {
		return 0, err
	}
	defer gw.Close()

	ttl := s.MaxTTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	if deadline, ok := ctx.Deadline(); ok {
		if left := time.Until(deadline) + 2*time.Minute; left < ttl {
			ttl = left
		}
	}
	jobID := make([]byte, 12)
	if _, err := rand.Read(jobID); err != nil {
		return 0, err
	}
	sb, err := s.Client.Create(ctx, sandboxd.CreateRequest{
		TemplateID: s.Template,
		Timeout:    ttl,
		Metadata: map[string]string{
			"job_id": "stagehand-" + hex.EncodeToString(jobID), "attempt": "1", "lifecycle_generation": "0",
			"owner": "stagehand",
		},
	})
	if err != nil {
		return 0, err
	}
	// Destroy even if ctx is already done (over budget, timeout).
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := s.Client.Destroy(dctx, sb.ID); err != nil && s.Log != nil {
			s.Log.Error("destroy sandbox", "sandbox", sb.ID, "err", err)
		}
	}()
	if req.Track != nil {
		if err := req.Track(sandboxPrefix + sb.ID); err != nil {
			return 0, fmt.Errorf("record sandbox: %w", err)
		}
	}
	if req.Stderr != nil {
		fmt.Fprintf(req.Stderr, "stagehand: sandbox %s (template %s)\n", sb.ID, s.Template)
	}

	var setupOut bytes.Buffer
	code, err := s.Client.Exec(ctx, sb, sandboxd.Process{Cmd: "/bin/mkdir", Args: []string{"-p", guestWork, guestAgentDir}}, &setupOut, &setupOut)
	if err != nil || code != 0 {
		return 0, fmt.Errorf("prepare sandbox: exit %d, %v: %s", code, err, strings.TrimSpace(setupOut.String()))
	}
	for name, data := range req.Files {
		if err := s.Client.Upload(ctx, sb, path.Join(guestWork, name), data); err != nil {
			return 0, err
		}
	}

	lines := &lineWriter{onLine: onLine}
	code, err = s.Client.Exec(ctx, sb, sandboxd.Process{
		Cmd:  s.GuestOmp,
		Args: req.Args,
		Dir:  guestWork,
		Env: map[string]string{
			"HOME":                guestHome,
			"PATH":                "/usr/local/bin:/usr/bin:/bin",
			"LANG":                "C.UTF-8",
			"PI_CODING_AGENT_DIR": guestAgentDir,
			"PI_NO_TITLE":         "1",
			"ANTHROPIC_BASE_URL":  s.GuestGatewayURL,
			"ANTHROPIC_API_KEY":   token,
			"NO_PROXY":            "10.0.2.2,127.0.0.1,localhost",
		},
	}, lines, req.Stderr)
	lines.flush()
	if s.Log != nil {
		s.Log.Info("sandbox run finished", "sandbox", sb.ID, "exit", code, "model_requests", gw.Requests(), "err", err)
	}
	if ctx.Err() != nil {
		return code, nil // the caller reports why it stopped
	}
	return code, err
}

// Cleanup destroys a VM left by a crashed runner.
func (s Sandboxd) Cleanup(ctx context.Context, resource string) error {
	id, ok := strings.CutPrefix(resource, sandboxPrefix)
	if !ok {
		return nil
	}
	return s.Client.Destroy(ctx, id)
}

// lineWriter splits a byte stream into lines.
type lineWriter struct {
	onLine func([]byte)
	buf    []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.onLine(w.buf[:i])
		w.buf = w.buf[i+1:]
	}
	if len(w.buf) > 64<<20 {
		return 0, io.ErrShortBuffer
	}
	return len(p), nil
}

func (w *lineWriter) flush() {
	if len(w.buf) > 0 {
		w.onLine(w.buf)
		w.buf = nil
	}
}
