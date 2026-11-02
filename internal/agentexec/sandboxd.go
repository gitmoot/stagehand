package agentexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
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
	// sandboxJobPrefix marks a tracked resource as the sandboxd job id a VM
	// is created under.
	sandboxJobPrefix = "sandboxd-job:"
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
	jobBytes := make([]byte, 12)
	if _, err := rand.Read(jobBytes); err != nil {
		return 0, err
	}
	jobID := "stagehand-" + hex.EncodeToString(jobBytes)
	resource := sandboxJobPrefix + jobID
	// Record the job id BEFORE asking for a VM: if the create response is
	// lost, or the runner dies mid-create, cleanup finds the VM by its
	// metadata.
	if req.Track != nil {
		if err := req.Track(resource); err != nil {
			return 0, fmt.Errorf("record sandbox job: %w", err)
		}
	}
	// However Run ends, destroy whatever was created under jobID, confirm it
	// is gone, and only then release the record. A failure leaves it for the
	// engine's cleanup sweep.
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
		defer cancel()
		done, err := s.destroyJob(dctx, jobID)
		switch {
		case done && req.Release != nil:
			req.Release(resource)
		case !done && s.Log != nil:
			s.Log.Error("sandbox not confirmed destroyed; the cleanup sweep will retry", "job", jobID, "err", err)
		}
	}()
	sb, err := s.Client.Create(ctx, sandboxd.CreateRequest{
		TemplateID: s.Template,
		Timeout:    ttl,
		Metadata: map[string]string{
			"job_id": jobID, "attempt": "1", "lifecycle_generation": "0", "owner": "stagehand",
		},
	})
	if err != nil {
		return 0, err
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

// Cleanup destroys every VM created under a recorded job id and reports done
// once a complete listing shows none left. A failed list or destroy leaves
// the record pending, and the engine retries it.
func (s Sandboxd) Cleanup(ctx context.Context, resource string) (bool, error) {
	jobID, ok := strings.CutPrefix(resource, sandboxJobPrefix)
	if !ok {
		return false, nil
	}
	return s.destroyJob(ctx, jobID)
}

// destroyJob destroys the VMs whose metadata carries jobID and confirms with
// a fresh listing that none is left.
func (s Sandboxd) destroyJob(ctx context.Context, jobID string) (bool, error) {
	list, err := s.Client.List(ctx)
	if err != nil {
		return false, err
	}
	var errs []error
	for _, sb := range list {
		if sb.Metadata["job_id"] != jobID {
			continue
		}
		if err := s.Client.Destroy(ctx, sb.ID); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return false, errors.Join(errs...)
	}
	list, err = s.Client.List(ctx)
	if err != nil {
		return false, err
	}
	for _, sb := range list {
		if sb.Metadata["job_id"] == jobID {
			return false, fmt.Errorf("sandbox %s for %s still listed after destroy", sb.ID, jobID)
		}
	}
	return true, nil
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
