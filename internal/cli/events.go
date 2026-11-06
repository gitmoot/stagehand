package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/gitmoot/stagehand/internal/engine"
	"github.com/gitmoot/stagehand/internal/homelock"
	"github.com/gitmoot/stagehand/internal/store"
	"github.com/gitmoot/stagehand/internal/trigger"
)

// trigger queues an event from the local command line. It is trusted (the
// caller can already read the home), so it needs no signature.
func (a *app) trigger(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("trigger", flag.ContinueOnError)
	eventID := fs.String("event-id", "", "idempotency key; the same key yields one run")
	payloadFile := fs.String("payload-file", "", "payload file (default: stdin)")
	contentType := fs.String("content-type", "", "payload media type")
	wait := fs.Bool("wait", false, "wait for the event's run to finish")
	asJSON := fs.Bool("json", false, "")
	pos, err := parse(fs, args, 1, "trigger <name> [--event-id ID] [--payload-file F] [--content-type T] [--wait] [--json]")
	if err != nil {
		return err
	}
	var payload io.Reader = os.Stdin
	if *payloadFile != "" {
		f, err := os.Open(*payloadFile)
		if err != nil {
			return err
		}
		defer f.Close()
		payload = f
	}
	res, err := a.engine.Trigger(ctx, engine.EventRequest{
		Pipeline: pos[0], Key: *eventID, ContentType: *contentType, Source: engine.SourceCLI,
		Payload: payload, MaxBytes: a.cfg.Events.PayloadLimit(),
	})
	if err != nil {
		return err
	}
	ev := res.Event
	if res.Duplicate {
		fmt.Fprintf(a.stderr, "event %q already received (event %d, %s); no new run\n", ev.Key, ev.ID, ev.State)
	} else {
		fmt.Fprintf(a.stderr, "event %d queued (%d bytes)\n", ev.ID, ev.PayloadBytes)
	}
	if !*wait {
		if *asJSON {
			return a.printJSON(map[string]any{"duplicate": res.Duplicate, "event": engine.EventRefOf(ev), "state": ev.State, "run_id": ev.RunID})
		}
		fmt.Fprintf(a.stdout, "event %d: %s\n", ev.ID, ev.State)
		return nil
	}
	runID, err := a.drainEvent(ctx, ev.Pipeline, ev.ID)
	if err != nil {
		return err
	}
	return a.finish(ctx, runID, true, *asJSON)
}

// drainEvent waits until event id has a run that has settled, and returns
// the run. When no other process executes stages it does the work here,
// limited to the event's own pipeline (whose events run one at a time, in
// order), and stops as soon as that run settles: newer arrivals or other
// pipelines never keep it waiting.
func (a *app) drainEvent(ctx context.Context, pipeline string, id int64) (int64, error) {
	settled := func(ctx context.Context) bool {
		ev, err := a.store.GetEvent(ctx, id)
		if err != nil || ev.State != store.EventStarted {
			return ev.State == store.EventDropped
		}
		r, err := a.store.GetRun(ctx, ev.RunID)
		return err == nil && r.State != store.RunRunning
	}
	for {
		ev, err := a.store.GetEvent(ctx, id)
		if err != nil {
			return 0, err
		}
		if ev.State == store.EventDropped {
			return 0, fmt.Errorf("event %d was dropped", id)
		}
		if ev.State == store.EventStarted && settled(ctx) {
			return ev.RunID, nil
		}
		lock, err := homelock.TryAcquire(filepath.Join(a.home, "stagehand.lock"), homelock.ModeCLI)
		if err == nil {
			// Loop checks Done before dispatching anything, so if the run
			// settled while another process held the lock, nothing newer is
			// started here.
			rerr := a.engine.Recover(ctx)
			if rerr == nil {
				rerr = a.engine.Loop(ctx, engine.LoopOptions{Events: true, Pipeline: pipeline, Done: settled, UntilIdle: true})
			}
			lock.Release()
			if rerr != nil {
				return 0, rerr
			}
			continue
		}
		var held *homelock.HeldError
		if !errors.As(err, &held) {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(lockPoll):
		}
	}
}

func (a *app) events(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	pipeline := fs.String("pipeline", "", "only this pipeline")
	state := fs.String("state", "", "queued, started or dropped")
	limit := fs.Int("limit", 50, "")
	asJSON := fs.Bool("json", false, "")
	if _, err := parse(fs, args, 0, "events [--pipeline NAME] [--state S] [--limit N] [--json]"); err != nil {
		return err
	}
	evs, err := a.store.ListEvents(ctx, *pipeline, *state, *limit)
	if err != nil {
		return err
	}
	if *asJSON {
		type row struct {
			*engine.EventRef
			Pipeline string `json:"pipeline"`
			State    string `json:"state"`
			RunID    int64  `json:"run_id,omitempty"`
		}
		rows := []row{}
		for _, e := range evs {
			rows = append(rows, row{EventRef: engine.EventRefOf(e), Pipeline: e.Pipeline, State: e.State, RunID: e.RunID})
		}
		return a.printJSON(rows)
	}
	tw := tabwriter.NewWriter(a.stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "EVENT\tPIPELINE\tKEY\tSTATE\tRUN\tRECEIVED\tBYTES\tSOURCE")
	for _, e := range evs {
		run := "-"
		if e.RunID != 0 {
			run = "#" + strconv.FormatInt(e.RunID, 10)
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", e.ID, e.Pipeline, e.Key, e.State, run, timeText(&e.ReceivedAt), e.PayloadBytes, e.Source)
	}
	return tw.Flush()
}

func (a *app) dropEvent(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("drop-event", flag.ContinueOnError), args, 1, "drop-event <event-id>")
	if err != nil {
		return err
	}
	id, err := strconv.ParseInt(pos[0], 10, 64)
	if err != nil {
		return fmt.Errorf("event id %q is not a number", pos[0])
	}
	ev, err := a.engine.DropEvent(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "event %d (%s) dropped; its payload is deleted\n", ev.ID, ev.Key)
	return nil
}

func (a *app) purge(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("purge", flag.ContinueOnError), args, 1, "purge <run-id>")
	if err != nil {
		return err
	}
	id, err := strconv.ParseInt(pos[0], 10, 64)
	if err != nil {
		return fmt.Errorf("run id %q is not a number", pos[0])
	}
	if err := a.engine.Purge(ctx, id); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "run #%d: files and payload deleted; it can no longer be resumed\n", id)
	return nil
}

// triggerServer serves the signed event endpoint.
func (a *app) triggerServer(addr string, allowRemote bool) (*http.Server, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("trigger-addr: %v", err)
	}
	if ip := net.ParseIP(host); !allowRemote && host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("trigger-addr %s is not loopback; pass --trigger-allow-remote to expose it", addr)
	}
	secretFile := a.cfg.Events.SecretFile
	if secretFile == "" {
		return nil, errors.New("trigger-addr needs events.secret_file in the runner config (a 0600 file with the HMAC key)")
	}
	if _, err := readSecretFile(secretFile); err != nil {
		return nil, fmt.Errorf("events.secret_file: %w", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	trigger.Handler{
		Engine:   a.engine,
		Secret:   func() ([]byte, error) { s, err := readSecretFile(secretFile); return []byte(s), err },
		MaxBytes: a.cfg.Events.PayloadLimit(),
		Log:      a.log,
	}.Routes(mux)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 10 * time.Minute}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("trigger server", "err", err)
		}
	}()
	a.log.Info("trigger endpoint", "url", "http://"+ln.Addr().String()+"/v1/pipelines/{name}/events")
	return srv, nil
}
