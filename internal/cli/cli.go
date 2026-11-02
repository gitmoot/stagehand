// Package cli is the stagehand command line.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/gitmoot/stagehand/internal/agentexec"
	"github.com/gitmoot/stagehand/internal/buildinfo"
	"github.com/gitmoot/stagehand/internal/config"
	"github.com/gitmoot/stagehand/internal/engine"
	"github.com/gitmoot/stagehand/internal/homelock"
	"github.com/gitmoot/stagehand/internal/sandboxd"
	"github.com/gitmoot/stagehand/internal/shellexec"
	"github.com/gitmoot/stagehand/internal/spec"
	"github.com/gitmoot/stagehand/internal/stage"
	"github.com/gitmoot/stagehand/internal/store"
)

const usage = `stagehand: a small scheduled pipeline runner.

Usage:
  stagehand [--home DIR] <command> [flags] [args]

Pipelines:
  add <spec.yaml> [--replace] [--enable]   register (or update) a pipeline
  list [--json]                            pipelines with schedule and last run
  show <name|run-id> [--json]              a pipeline, or one run with its stages
  enable <name> | disable <name>           turn the schedule on or off
  remove <name>                            delete a pipeline (its runs stay)

Runs:
  run <name> [--wait] [--json]             start a run (executes here unless serve is running)
  runs <name> [--limit N] [--json]         newest runs of a pipeline
  resume <run-id> [--from STAGE] [--wait]  re-run a parked run from a stage
  cancel <run-id>                          stop a running run

Service:
  serve [--status-addr 127.0.0.1:PORT] [--poll 5s]  run schedules and stages until stopped
  tick                                     one pass: fire due schedules, run them to the end
  gateway-proxy [--listen ADDR] [--upstream URL] [--token-file FILE]
                                           model credential proxy for sandboxed agent stages
  status [--json]                          last and next run per pipeline
  version

The home (state database, run logs and work files) is --home, else
$STAGEHAND_HOME, else /var/lib/stagehand for root or
$XDG_STATE_HOME/stagehand (~/.local/state/stagehand).

Runner settings (how agent stages execute) are read from --config, else
$STAGEHAND_CONFIG, else <home>/config.yaml if it exists.
`

// Main runs the CLI and returns the process exit code.
func Main(args []string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("stagehand", flag.ContinueOnError)
	global.SetOutput(stderr)
	home := global.String("home", "", "state directory")
	configPath := global.String("config", os.Getenv("STAGEHAND_CONFIG"), "runner settings file")
	global.Usage = func() { fmt.Fprint(stderr, usage) }
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	rest := global.Args()
	if len(rest) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, cmdArgs := rest[0], rest[1:]
	if cmd == "version" {
		fmt.Fprintln(stdout, buildinfo.String())
		return 0
	}
	if cmd == "help" || cmd == "-h" || cmd == "--help" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cmd == "gateway-proxy" {
		// Runs on the host holding the omp auth-gateway token; needs no home.
		if err := gatewayProxy(ctx, cmdArgs, stderr); err != nil {
			fmt.Fprintln(stderr, "stagehand:", err)
			return 1
		}
		return 0
	}

	a, err := open(ctx, *home, *configPath, stderr)
	if err != nil {
		fmt.Fprintln(stderr, "stagehand:", err)
		return 1
	}
	defer a.close()
	a.stdout = stdout

	commands := map[string]func(context.Context, []string) error{
		"add": a.add, "list": a.list, "show": a.show, "enable": a.enable, "disable": a.disable,
		"remove": a.remove, "run": a.run, "runs": a.runs, "resume": a.resume, "cancel": a.cancel,
		"serve": a.serve, "tick": a.tick, "status": a.status,
	}
	fn, ok := commands[cmd]
	if !ok {
		fmt.Fprintf(stderr, "stagehand: unknown command %q\n\n%s", cmd, usage)
		return 2
	}
	if err := fn(ctx, cmdArgs); err != nil {
		var ec exitCode
		if errors.As(err, &ec) {
			return int(ec)
		}
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintln(stderr, "stagehand:", err)
		return 1
	}
	return 0
}

// exitCode ends the command with a non-zero status without printing an error.
type exitCode int

func (e exitCode) Error() string { return fmt.Sprintf("exit %d", int(e)) }

type app struct {
	home   string
	store  *store.Store
	engine *engine.Engine
	stdout io.Writer
	stderr io.Writer
	log    *slog.Logger
}

// DefaultHome resolves the state directory.
func DefaultHome() (string, error) {
	if h := os.Getenv("STAGEHAND_HOME"); h != "" {
		return h, nil
	}
	if os.Geteuid() == 0 {
		return "/var/lib/stagehand", nil
	}
	if x := os.Getenv("XDG_STATE_HOME"); x != "" {
		return filepath.Join(x, "stagehand"), nil
	}
	h, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(h, ".local", "state", "stagehand"), nil
}

func open(ctx context.Context, home, configPath string, stderr io.Writer) (*app, error) {
	if home == "" {
		var err error
		if home, err = DefaultHome(); err != nil {
			return nil, err
		}
	}
	home, err := filepath.Abs(home)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, err
	}
	required := configPath != ""
	if configPath == "" {
		configPath = filepath.Join(home, "config.yaml")
	}
	cfg, err := config.Load(configPath, required)
	if err != nil {
		return nil, err
	}
	st, err := store.Open(ctx, filepath.Join(home, "stagehand.db"))
	if err != nil {
		return nil, err
	}
	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	executors := map[spec.Kind]stage.Executor{spec.KindShell: shellexec.Executor{}}
	switch cfg.Agent.Executor {
	case config.ExecutorLocal:
		executors[spec.KindAgent] = agentexec.Executor{Backend: agentexec.Local{
			Omp: cfg.Agent.Omp,
			Dir: filepath.Join(home, "agent-scratch"),
			Env: baseEnv(),
		}}
	case config.ExecutorSandboxd:
		sc := cfg.Agent.Sandboxd
		key, err := readSecretFile(sc.APIKeyFile)
		if err != nil {
			st.Close()
			return nil, fmt.Errorf("agent.sandboxd.api_key_file: %w", err)
		}
		executors[spec.KindAgent] = agentexec.Executor{Backend: agentexec.Sandboxd{
			Client:          &sandboxd.Client{BaseURL: strings.TrimRight(sc.URL, "/"), APIKey: key},
			Template:        sc.Template,
			GuestOmp:        sc.GuestOmpPath(),
			MaxTTL:          sc.MaxTTLDuration(),
			GatewayListen:   cfg.Agent.ModelGateway.Listen,
			GuestGatewayURL: cfg.Agent.ModelGateway.GuestURL,
			GatewayUpstream: cfg.Agent.ModelGateway.Upstream,
			Log:             log,
		}}
	}
	eng := engine.New(st, home, executors, baseEnv(), log)
	return &app{home: home, store: st, engine: eng, stderr: stderr, log: log}, nil
}

func (a *app) close() { _ = a.store.Close() }

// baseEnv is the non-secret environment every shell stage gets.
func baseEnv() []string {
	path := os.Getenv("PATH")
	if path == "" {
		path = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	}
	env := []string{"PATH=" + path, "LANG=C.UTF-8"}
	if h := os.Getenv("HOME"); h != "" {
		env = append(env, "HOME="+h)
	}
	if tz := os.Getenv("TZ"); tz != "" {
		env = append(env, "TZ="+tz)
	}
	return env
}

// parse parses flags that may appear before or after positional arguments.
func parse(fs *flag.FlagSet, args []string, wantPos int, usage string) ([]string, error) {
	fs.SetOutput(io.Discard)
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, fmt.Errorf("%v\nusage: stagehand %s", err, usage)
		}
		if fs.NArg() == 0 {
			break
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
	if len(pos) != wantPos {
		return nil, fmt.Errorf("usage: stagehand %s", usage)
	}
	return pos, nil
}

func (a *app) printJSON(v any) error {
	enc := json.NewEncoder(a.stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// ---- pipeline commands ----

func (a *app) add(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("add", flag.ContinueOnError)
	replace := fs.Bool("replace", false, "")
	enable := fs.Bool("enable", false, "")
	pos, err := parse(fs, args, 1, "add <spec.yaml> [--replace] [--enable]")
	if err != nil {
		return err
	}
	path, err := filepath.Abs(pos[0])
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	p, err := a.engine.AddPipeline(ctx, raw, filepath.Dir(path), *replace, *enable)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "pipeline %s registered (spec %s)%s\n", p.Name, p.SpecHash[:12], dueText(p))
	return nil
}

func dueText(p store.Pipeline) string {
	switch {
	case !p.Enabled:
		return "; disabled"
	case p.NextDueAt == nil:
		return "; enabled, no schedule (manual runs only)"
	default:
		return "; enabled, next run " + p.NextDueAt.Local().Format(time.RFC3339)
	}
}

func (a *app) setEnabled(ctx context.Context, args []string, on bool) error {
	name := "disable"
	if on {
		name = "enable"
	}
	pos, err := parse(flag.NewFlagSet(name, flag.ContinueOnError), args, 1, name+" <name>")
	if err != nil {
		return err
	}
	p, err := a.engine.SetEnabled(ctx, pos[0], on)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "pipeline %s%s\n", p.Name, dueText(p))
	return nil
}

func (a *app) enable(ctx context.Context, args []string) error { return a.setEnabled(ctx, args, true) }
func (a *app) disable(ctx context.Context, args []string) error {
	return a.setEnabled(ctx, args, false)
}

func (a *app) remove(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("remove", flag.ContinueOnError), args, 1, "remove <name>")
	if err != nil {
		return err
	}
	if err := a.engine.RemovePipeline(ctx, pos[0]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "pipeline %s removed; its runs are kept\n", pos[0])
	return nil
}

func (a *app) list(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "")
	if _, err := parse(fs, args, 0, "list [--json]"); err != nil {
		return err
	}
	st, err := a.engine.Status(ctx, buildinfo.Version)
	if err != nil {
		return err
	}
	if *asJSON {
		return a.printJSON(st.Pipelines)
	}
	return a.printStatusTable(st)
}

func (a *app) status(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "")
	if _, err := parse(fs, args, 0, "status [--json]"); err != nil {
		return err
	}
	st, err := a.engine.Status(ctx, buildinfo.Version)
	if err != nil {
		return err
	}
	if *asJSON {
		return a.printJSON(st)
	}
	return a.printStatusTable(st)
}

func (a *app) printStatusTable(st engine.Status) error {
	tw := tabwriter.NewWriter(a.stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PIPELINE\tENABLED\tSCHEDULE\tNEXT RUN\tLAST RUN\tSTATE\tCOST")
	for _, p := range st.Pipelines {
		sched := "manual"
		if p.Schedule != nil {
			sched = "every " + p.Schedule.Interval
		}
		last, state, cost := "-", "-", "-"
		if p.LastRun != nil {
			last = fmt.Sprintf("#%d %s", p.LastRun.ID, p.LastRun.CreatedAt.Local().Format("2006-01-02 15:04"))
			state = p.LastRun.State
			cost = fmt.Sprintf("%.2f", p.LastRun.Cost)
		}
		fmt.Fprintf(tw, "%s\t%v\t%s\t%s\t%s\t%s\t%s\n", p.Name, p.Enabled, sched, timeText(p.NextDueAt), last, state, cost)
	}
	return tw.Flush()
}

func timeText(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04")
}

func (a *app) show(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "")
	pos, err := parse(fs, args, 1, "show <name|run-id> [--json]")
	if err != nil {
		return err
	}
	if id, err := strconv.ParseInt(pos[0], 10, 64); err == nil {
		d, err := a.engine.Run(ctx, id)
		if err != nil {
			return err
		}
		if *asJSON {
			return a.printJSON(d)
		}
		return a.printRun(d)
	}
	p, err := a.store.GetPipeline(ctx, pos[0])
	if err != nil {
		return err
	}
	runs, err := a.engine.Runs(ctx, p.Name, 10)
	if err != nil {
		return err
	}
	if *asJSON {
		return a.printJSON(map[string]any{
			"name": p.Name, "enabled": p.Enabled, "next_due_at": p.NextDueAt, "spec_hash": p.SpecHash,
			"source_dir": p.SourceDir, "spec_yaml": string(p.SpecYAML), "runs": runs,
		})
	}
	fmt.Fprintf(a.stdout, "pipeline: %s\nenabled: %v\nnext run: %s\nspec: %s (from %s)\n\n", p.Name, p.Enabled, timeText(p.NextDueAt), p.SpecHash[:12], p.SourceDir)
	fmt.Fprint(a.stdout, strings.TrimRight(string(p.SpecYAML), "\n")+"\n\n")
	return a.printRuns(runs)
}

func (a *app) printRuns(runs []engine.RunSummary) error {
	tw := tabwriter.NewWriter(a.stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RUN\tTRIGGER\tSTATE\tSTARTED\tFINISHED\tCOST\tHALTED AT")
	for _, r := range runs {
		halt := "-"
		if r.HaltStage != "" {
			halt = r.HaltStage + ": " + r.HaltReason
		}
		fmt.Fprintf(tw, "#%d\t%s\t%s\t%s\t%s\t%.2f\t%s\n", r.ID, r.Trigger, r.State, timeText(&r.CreatedAt), timeText(r.FinishedAt), r.Cost, halt)
	}
	return tw.Flush()
}

func (a *app) printRun(d engine.RunDetail) error {
	fmt.Fprintf(a.stdout, "run #%d of %s (%s): %s\n", d.ID, d.Pipeline, d.Trigger, d.State)
	if d.HaltStage != "" {
		fmt.Fprintf(a.stdout, "halted at %s: %s\n", d.HaltStage, d.HaltReason)
	}
	fmt.Fprintf(a.stdout, "started %s, finished %s, cost %.4f (%d in / %d out tokens)\n\n",
		timeText(&d.CreatedAt), timeText(d.FinishedAt), d.Cost, d.InputTokens, d.OutputTokens)
	tw := tabwriter.NewWriter(a.stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STAGE\tKIND\tSTATE\tTRIES\tEXIT\tCOST\tSUMMARY\tLOG")
	for _, s := range d.Stages {
		exit, cost, logPath := "-", 0.0, "-"
		for _, at := range s.Attempts {
			cost += at.Cost
		}
		if n := len(s.Attempts); n > 0 {
			last := s.Attempts[n-1]
			if last.ExitCode != nil {
				exit = strconv.Itoa(*last.ExitCode)
			}
			logPath = last.LogPath
		}
		summary := s.Summary
		if len(s.Needs) > 0 {
			summary += " (needs: " + strings.Join(s.Needs, ", ") + ")"
		}
		if len(summary) > 80 {
			summary = summary[:80] + "…"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%.4f\t%s\t%s\n", s.ID, s.Kind, s.State, len(s.Attempts), exit, cost, summary, logPath)
	}
	return tw.Flush()
}

func (a *app) runs(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("runs", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "")
	limit := fs.Int("limit", 20, "")
	pos, err := parse(fs, args, 1, "runs <name> [--limit N] [--json]")
	if err != nil {
		return err
	}
	if _, err := a.store.GetPipeline(ctx, pos[0]); err != nil {
		return err
	}
	runs, err := a.engine.Runs(ctx, pos[0], *limit)
	if err != nil {
		return err
	}
	if *asJSON {
		return a.printJSON(runs)
	}
	return a.printRuns(runs)
}

// ---- run commands ----

func (a *app) run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	wait := fs.Bool("wait", false, "")
	asJSON := fs.Bool("json", false, "")
	pos, err := parse(fs, args, 1, "run <name> [--wait] [--json]")
	if err != nil {
		return err
	}
	id, err := a.engine.StartRun(ctx, pos[0], "manual")
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stderr, "run #%d created\n", id)
	return a.finish(ctx, id, *wait, *asJSON)
}

func (a *app) resume(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("resume", flag.ContinueOnError)
	from := fs.String("from", "", "")
	wait := fs.Bool("wait", false, "")
	asJSON := fs.Bool("json", false, "")
	pos, err := parse(fs, args, 1, "resume <run-id> [--from STAGE] [--wait] [--json]")
	if err != nil {
		return err
	}
	id, err := strconv.ParseInt(pos[0], 10, 64)
	if err != nil {
		return fmt.Errorf("run id %q is not a number", pos[0])
	}
	if err := a.engine.Resume(ctx, id, *from); err != nil {
		return err
	}
	fmt.Fprintf(a.stderr, "run #%d resumed\n", id)
	return a.finish(ctx, id, *wait, *asJSON)
}

func (a *app) cancel(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("cancel", flag.ContinueOnError), args, 1, "cancel <run-id>")
	if err != nil {
		return err
	}
	id, err := strconv.ParseInt(pos[0], 10, 64)
	if err != nil {
		return fmt.Errorf("run id %q is not a number", pos[0])
	}
	if err := a.engine.Cancel(ctx, id); err != nil {
		return err
	}
	executed, err := a.execute(ctx, map[int64]bool{id: true})
	if err != nil {
		return err
	}
	if !executed {
		fmt.Fprintf(a.stdout, "cancel of run #%d requested; the running stagehand process stops it\n", id)
		return nil
	}
	fmt.Fprintf(a.stdout, "run #%d cancelled\n", id)
	return nil
}

// finish executes the run here if no other process holds the lock;
// otherwise it leaves it to that process and, with wait, polls until the run
// settles. It prints the run and fails unless it succeeded.
func (a *app) finish(ctx context.Context, id int64, wait, asJSON bool) error {
	executed, err := a.execute(ctx, map[int64]bool{id: true})
	if err != nil {
		return err
	}
	if !executed {
		if !wait {
			fmt.Fprintf(a.stdout, "run #%d queued; the running stagehand process executes it (stagehand show %d)\n", id, id)
			return nil
		}
		for {
			r, err := a.store.GetRun(ctx, id)
			if err != nil {
				return err
			}
			if r.State != store.RunRunning {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
	d, err := a.engine.Run(context.WithoutCancel(ctx), id)
	if err != nil {
		return err
	}
	if asJSON {
		err = a.printJSON(d)
	} else {
		err = a.printRun(d)
	}
	if err != nil {
		return err
	}
	if d.State != store.RunSucceeded {
		return exitCode(1)
	}
	return nil
}

// execute runs the selected runs to the end in this process if it can take
// the home lock. It reports false if another process holds it.
func (a *app) execute(ctx context.Context, only map[int64]bool) (bool, error) {
	lock, err := homelock.TryAcquire(filepath.Join(a.home, "stagehand.lock"))
	if errors.Is(err, homelock.ErrHeld) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer lock.Release()
	if err := a.engine.Recover(ctx); err != nil {
		return true, err
	}
	err = a.engine.Loop(ctx, engine.LoopOptions{Only: only, UntilIdle: true})
	if errors.Is(err, context.Canceled) {
		return true, errors.New("interrupted; the run is parked and can be resumed")
	}
	return true, err
}

// ---- service ----

func (a *app) serve(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	statusAddr := fs.String("status-addr", "", "loopback address for GET /status (off when empty)")
	poll := fs.Duration("poll", engine.DefaultPoll, "")
	if _, err := parse(fs, args, 0, "serve [--status-addr 127.0.0.1:PORT] [--poll 5s]"); err != nil {
		return err
	}
	lock, err := homelock.TryAcquire(filepath.Join(a.home, "stagehand.lock"))
	if err != nil {
		return err
	}
	defer lock.Release()
	if err := a.engine.Recover(ctx); err != nil {
		return err
	}
	if *statusAddr != "" {
		srv, err := a.statusServer(*statusAddr)
		if err != nil {
			return err
		}
		defer srv.Close()
	}
	a.engine.Poll = *poll
	a.log.Info("serving", "home", a.home, "version", buildinfo.String())
	err = a.engine.Loop(ctx, engine.LoopOptions{Schedule: true})
	if errors.Is(err, context.Canceled) {
		a.log.Info("stopped")
		return nil
	}
	return err
}

func (a *app) tick(ctx context.Context, args []string) error {
	if _, err := parse(flag.NewFlagSet("tick", flag.ContinueOnError), args, 0, "tick"); err != nil {
		return err
	}
	lock, err := homelock.TryAcquire(filepath.Join(a.home, "stagehand.lock"))
	if errors.Is(err, homelock.ErrHeld) {
		fmt.Fprintln(a.stdout, "another stagehand process is running; nothing to do")
		return nil
	}
	if err != nil {
		return err
	}
	defer lock.Release()
	if err := a.engine.Recover(ctx); err != nil {
		return err
	}
	return a.engine.Loop(ctx, engine.LoopOptions{Schedule: true, UntilIdle: true})
}

// statusServer serves GET /status on a loopback address only.
func (a *app) statusServer(addr string) (*http.Server, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("status-addr: %v", err)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("status-addr %s: only loopback addresses are allowed", addr)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		st, err := a.engine.Status(r.Context(), buildinfo.Version)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("status server", "err", err)
		}
	}()
	a.log.Info("status endpoint", "url", "http://"+ln.Addr().String()+"/status")
	return srv, nil
}
