package run

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ray-project/kuberay/release-test/pkg/util"
	"github.com/ray-project/kuberay/release-test/pkg/util/report"
)

// unitRun is the state of one unit while it runs.
type unitRun struct {
	work   string // the steps' working directory
	out    string // holds the current step's run output; steps see it as $OUT
	groups []int  // process groups started, killed when the unit ends
}

// runUnit runs one unit and returns the step it failed at, 0 for a pass. On Ctrl-C it still runs
// the cleanup, then returns errInterrupted without a report, so the next run repeats the unit.
func (r *runner) runUnit(ctx context.Context, item util.Item, unit util.Unit) (failed int, err error) {
	dir := filepath.Join(r.path.Results, unit.ID)
	if err := os.RemoveAll(dir); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Join(dir, util.StepsDir), 0o750); err != nil {
		return 0, err
	}
	work, err := os.MkdirTemp("", "release-test-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(work)
	state := &unitRun{work: work, out: filepath.Join(work, ".out")}

	defer state.killGroups()
	before, snapErr := r.snapshot(ctx)

	start := time.Now()

	rep := unitReport{
		ID:        unit.ID,
		Source:    cmp.Or(item.Source, "procedure in items.yaml"),
		SourceSHA: unit.SourceSHA,
		Started:   start.UTC().Format(time.RFC3339),
		Notes:     unit.Notes,
		SweepErr:  snapErr,
	}

	brief := ""

	for i, step := range unit.Steps {
		num := fmt.Sprintf("%02d", i+1)
		logPath := filepath.Join(dir, util.StepsDir, num+".log")
		failure, took, err := state.step(ctx, logPath, step)
		if err != nil {
			return 0, err
		}

		result := util.StatusPass

		if failure != "" {
			result = util.StatusFail + ": " + failure
		}

		rep.Steps = append(rep.Steps, stepRow{
			Num:    i + 1,
			Name:   cmp.Or(step.Name, step.Run),
			Result: result,
			Took:   took,
			Log:    util.StepsDir + "/" + num + ".log",
			Lines:  countLines(logPath),
		})

		if failure != "" {
			failed = i + 1
			brief = fmt.Sprintf("step %d %s", failed, failure)
			break
		}
	}
	if failed > 0 && ctx.Err() == nil {
		for i, step := range unit.Steps[failed:] {
			rep.Steps = append(rep.Steps, stepRow{Num: failed + i + 1, Name: cmp.Or(step.Name, step.Run)})
		}
		r.evidence(ctx, filepath.Join(dir, "evidence"))
	}

	cctx, stop := cleanupContext(ctx)
	defer stop()

	cleanupLog, err := os.OpenFile(filepath.Join(dir, util.StepsDir, "cleanup.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 0, err
	}

	defer cleanupLog.Close()

	for _, command := range unit.Cleanup {
		fmt.Fprint(cleanupLog, prefixLines(command, "$ ", "  "))
		start := time.Now()
		code, timedOut := state.shell(cctx, util.DefaultTimeout*time.Second, cleanupLog, command)
		if timedOut {
			fmt.Fprintf(cleanupLog, "# timed out after %ds\n\n", util.DefaultTimeout)
		} else {
			fmt.Fprintf(cleanupLog, "# exit %d after %ds\n\n", code, int(time.Since(start).Seconds()))
		}
	}
	state.killGroups()
	if snapErr == nil {
		rep.Swept = r.sweep(cctx, before, cleanupLog)
	} else {
		fmt.Fprintf(cleanupLog, "\nsweep skipped: %v\n", snapErr)
	}
	// Checked after the cleanup: a cancel during evidence or cleanup leaves no report either.
	if ctx.Err() != nil {
		if cctx.Err() != nil {
			fmt.Println("interrupted; cleanup stopped early, check the cluster for leftovers")
		} else {
			fmt.Println("interrupted; cleanup done")
		}
		return 0, errInterrupted
	}

	rep.Took = int(time.Since(start).Seconds())
	rep.Front = report.FrontMatter{Item: unit.ID, Status: util.StatusPass, Version: r.version, Brief: brief}
	if failed > 0 {
		rep.Front.Status, rep.Front.FailedStep = util.StatusFail, &failed
	}
	if err := os.WriteFile(filepath.Join(dir, util.ReportFile), []byte(report.Render("report.md.tmpl", rep)), 0o600); err != nil {
		return 0, err
	}
	if failed > 0 {
		fmt.Printf("fail at step %d (%ds)\n", failed, rep.Took)
	} else {
		fmt.Printf("pass (%ds)\n", rep.Took)
	}
	return failed, nil
}

// cleanupContext is ctx without its cancel, so the cleanup runs even when ctx is canceled before
// or during it. From that cancel on, the cleanup has 10 minutes and a second Ctrl-C stops it.
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopWatch := context.AfterFunc(ctx, func() {
		// Armed only now, so the Ctrl-C that canceled ctx does not also stop the cleanup.
		sctx, stop := signal.NotifyContext(cctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
		sctx, timeout := context.WithTimeout(sctx, 10*time.Minute)
		defer timeout()
		<-sctx.Done()
		cancel()
	})
	return cctx, func() {
		stopWatch()
		cancel()
	}
}

// countLines of a log, so the triage skill can read its tail without reading it all.
func countLines(path string) int {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return 0
	}
	return bytes.Count(data, []byte("\n"))
}

// step runs one step and logs it; returns why it failed ("" when it passed) and the seconds it took.
func (u *unitRun) step(ctx context.Context, logPath string, step util.Step) (string, int, error) {
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", 0, err
	}
	defer log.Close()
	if step.Name != "" {
		fmt.Fprintf(log, "# %s\n", step.Name)
	}
	fmt.Fprint(log, prefixLines(step.Run, "$ ", "  "))
	if step.Check != "" {
		fmt.Fprint(log, prefixLines(step.Check, "# check: ", "#        "))
	}
	fmt.Fprintln(log)

	timeout := time.Duration(cmp.Or(step.Timeout, util.DefaultTimeout)) * time.Second
	start := time.Now()
	out, err := os.Create(u.out)
	if err != nil {
		return "", 0, err
	}
	code, timedOut := u.shell(ctx, timeout, out, step.Run)
	out.Close()
	if data, err := os.ReadFile(u.out); err == nil {
		_, _ = log.Write(data)
	}
	status := "run"
	if code == 0 && !timedOut && step.Check != "" {
		fmt.Fprint(log, "\n# check\n")
		code, timedOut = u.shell(ctx, timeout, log, step.Check)
		status = "check"
	}
	took := int(time.Since(start).Seconds())
	fmt.Fprintf(log, "\n# %s exit %d after %ds\n", status, code, took)
	switch {
	case timedOut:
		return fmt.Sprintf("%s timed out after %ds", status, int(timeout.Seconds())), took, nil
	case code != 0:
		return fmt.Sprintf("%s exit %d", status, code), took, nil
	}
	return "", took, nil
}

// shell runs a script with bash -e in its own process group, so a timeout kills everything it
// started and leftovers (port-forwards) die in killGroups. out must be a file: a pipe would make
// Wait block on those leftovers.
func (u *unitRun) shell(ctx context.Context, timeout time.Duration, out *os.File, script string) (code int, timedOut bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-ec", script)
	cmd.Dir = u.work
	cmd.Env = append(cmd.Environ(), "OUT="+u.out)
	cmd.Stdout, cmd.Stderr = out, out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	cmd.WaitDelay = 10 * time.Second
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(out, err)
		return -1, false
	}
	u.groups = append(u.groups, cmd.Process.Pid)
	_ = cmd.Wait()
	return cmd.ProcessState.ExitCode(), errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func (u *unitRun) killGroups() {
	for _, pgid := range u.groups {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
	u.groups = nil
}

// unitReport is what report.md.tmpl writes into a unit's report.md.
type unitReport struct {
	Front     report.FrontMatter
	ID        string
	Source    string
	SourceSHA string
	Started   string
	Took      int
	Steps     []stepRow
	Notes     []string
	Swept     []string // what the runner deleted after the unit's cleanup
	SweepErr  error    // why the cluster could not be listed before the unit, so nothing was swept
}

// stepRow is one step of a report; a step after the failed one has no Result.
type stepRow struct {
	Num, Took, Lines int
	Name, Result     string
	Log              string
}

// cliFunc runs kubectl or helm; tests swap in a fake cluster.
type cliFunc func(ctx context.Context, stdout, stderr io.Writer, name string, args ...string) error

func execCLI(ctx context.Context, stdout, stderr io.Writer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	return cmd.Run()
}

// output runs a command and returns its stdout; stderr is dropped (helm prints plugin warnings).
func (r *runner) output(ctx context.Context, name string, args ...string) (string, error) {
	var out strings.Builder
	err := r.cli(ctx, &out, io.Discard, name, args...)
	return strings.TrimSpace(out.String()), err
}

// toFile runs a command with stdout and stderr appended to file.
func (r *runner) toFile(ctx context.Context, file *os.File, name string, args ...string) {
	_ = r.cli(ctx, file, file, name, args...)
}

// toPath is toFile for the file at path, which it creates if missing.
func (r *runner) toPath(ctx context.Context, path, name string, args ...string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	r.toFile(ctx, file, name, args...)
}

// prefixLines prefixes the first line of text with first and the others with rest.
func prefixLines(text, first, rest string) string {
	return first + strings.ReplaceAll(strings.TrimRight(text, "\n"), "\n", "\n"+rest) + "\n"
}
