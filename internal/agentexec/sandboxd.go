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
	"strconv"
	"strings"
	"time"

	"github.com/gitmoot/stagehand/internal/modelgw"
	"github.com/gitmoot/stagehand/internal/sandboxd"
)

// createWindow bounds a create request; sandboxd's own template-ready limit
// is 3 minutes. settleMargin covers clock skew and sandboxd's 1s TTL sweep.
const (
	createWindow = 5 * time.Minute
	settleMargin = 5 * time.Minute
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
	// Record the job id BEFORE asking for a VM: if the create response is
	// lost, or the runner dies mid-create, cleanup finds the VM by its
	// metadata.
	resource := jobResource(jobID, time.Now(), ttl)
	if req.Track != nil {
		if err := req.Track(resource); err != nil {
			return 0, fmt.Errorf("record sandbox job: %w", err)
		}
	}
	// allocationKnown is true once sandboxd has definitively answered the
	// create (a VM id, or a 4xx refusal). Until then a VM may still appear.
	allocationKnown := false
	// However Run ends, destroy whatever was created under jobID and confirm
	// it is gone. The record is released only when the allocation is known;
	// otherwise the engine's cleanup sweep keeps checking until the settle
	// time recorded in the resource.
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
		defer cancel()
		gone, err := s.destroyJob(dctx, jobID)
		switch {
		case gone && allocationKnown && req.Release != nil:
			req.Release(resource)
		case !gone && s.Log != nil:
			s.Log.Error("sandbox not confirmed destroyed; the cleanup sweep will retry", "job", jobID, "err", err)
		case !allocationKnown && s.Log != nil:
			s.Log.Warn("sandbox create outcome unknown; the cleanup sweep keeps checking", "job", jobID)
		}
	}()
	createCtx, cancelCreate := context.WithTimeout(ctx, createWindow)
	sb, err := s.Client.Create(createCtx, sandboxd.CreateRequest{
		TemplateID: s.Template,
		Timeout:    ttl,
		Metadata: map[string]string{
			"job_id": jobID, "attempt": "1", "lifecycle_generation": "0", "owner": "stagehand",
		},
	})
	cancelCreate()
	var refused *sandboxd.HTTPError
	if err == nil || (errors.As(err, &refused) && refused.Status >= 400 && refused.Status < 500) {
		// The outcome is definite: swap the record for one without a settle
		// time, so a confirmed destroy can release it at once. Record the
		// new one before dropping the old.
		known := sandboxJobPrefix + jobID
		if req.Track == nil || req.Track(known) == nil {
			if req.Release != nil {
				req.Release(resource)
			}
			resource, allocationKnown = known, true
		}
	}
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

// MaxConcurrent is 1: the instance exposes one host port, which one run
// gateway at a time listens on.
func (Sandboxd) MaxConcurrent() int { return 1 }

// Cleanup destroys every VM created under a recorded job id. It reports done
// once a listing shows none left AND the recorded settle time has passed, so
// a create that sandboxd completes after the client gave up is still found.
// A failed list or destroy leaves the record pending, and the engine retries
// it.
func (s Sandboxd) Cleanup(ctx context.Context, resource string) (bool, error) {
	rest, ok := strings.CutPrefix(resource, sandboxJobPrefix)
	if !ok {
		return false, nil
	}
	jobID, settleText, _ := strings.Cut(rest, "@")
	gone, err := s.destroyJob(ctx, jobID)
	if err != nil || !gone {
		return false, err
	}
	settle, perr := strconv.ParseInt(settleText, 10, 64)
	if perr != nil {
		return true, nil // no settle time recorded: nothing more can be checked
	}
	return time.Now().Unix() >= settle, nil
}

// jobResource names a sandbox job and the time after which no VM can still
// appear or live under it: the create can take at most createWindow to
// settle server-side, and sandboxd destroys every VM at its TTL.
func jobResource(jobID string, now time.Time, ttl time.Duration) string {
	settle := now.Add(createWindow + ttl + settleMargin)
	return fmt.Sprintf("%s%s@%d", sandboxJobPrefix, jobID, settle.Unix())
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
