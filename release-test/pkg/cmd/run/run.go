package run

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
	"sigs.k8s.io/yaml"

	"github.com/ray-project/kuberay/release-test/pkg/util"
	"github.com/ray-project/kuberay/release-test/pkg/util/report"
	"github.com/ray-project/kuberay/release-test/pkg/util/source"
)

var (
	chartIndex     = "https://ray-project.github.io/kuberay-helm/index.yaml"
	errInterrupted = errors.New("interrupted; the unit has no report and runs again next time")
)

var (
	runLong = templates.LongDesc(`
		Run the units in release-test/manifest.yaml on the current kind cluster and write a report per
		Item, then SUMMARY.md, under release-test/results/TAG/.

		It first lints the manifest and checks that the tag's Helm charts are published. A unit whose
		source changed since it was planned is not run but reported as stale. It stops after three units
		in a row fail at their first step. Ctrl-C runs the current unit's cleanup and stops; that unit
		runs again next time.

		Without IDs it resumes, skipping Items that already have a pass or fail report; with IDs it runs
		exactly those. Steps see the tag as $KUBERAY_VERSION and the tag without the v as $CHART_VERSION.
	`)

	runExample = templates.Examples(`
		# Every Item
		go run ./release-test run --kuberay-version v1.8.0-rc.0

		# Re-run two Items
		go run ./release-test run --kuberay-version v1.8.0-rc.0 raycluster-quick-start ray-cluster-sample
	`)
)

type runOptions struct {
	paths   util.Paths
	version string
	ids     []string
}

func newRunOptions() *runOptions {
	return &runOptions{}
}

func NewRunCommand() *cobra.Command {
	options := newRunOptions()

	cmd := &cobra.Command{
		Use:          "run --kuberay-version TAG [ID...]",
		Short:        "Run the manifest on the current kind cluster",
		Long:         runLong,
		Example:      runExample,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(args); err != nil {
				return err
			}
			if err := options.Validate(); err != nil {
				return err
			}
			return options.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&options.version, "kuberay-version", "", "the tag under test, e.g. v1.8.0-rc.0; its Helm charts must be published")
	cobra.CheckErr(cmd.MarkFlagRequired("kuberay-version"))

	return cmd
}

func (options *runOptions) Complete(args []string) error {
	repoPaths, err := util.FindPaths()
	if err != nil {
		return err
	}
	repoPaths.Results = repoPaths.ResultsDir(options.version)
	options.paths = repoPaths
	options.ids = args
	return nil
}

func (options *runOptions) Validate() error {
	return util.ValidateVersion(options.version)
}

func (options *runOptions) Run(ctx context.Context) error {
	repoPaths, ids := options.paths, options.ids
	items, err := util.LoadItems(repoPaths.Items)
	if err != nil {
		return err
	}
	units, err := util.LoadManifest(repoPaths.Manifest)
	if err != nil {
		return err
	}
	if problems := util.LintManifest(items, units); len(problems) > 0 {
		util.PrintProblems(problems)
		return util.ErrLint
	}
	run := runner{
		path:    repoPaths,
		version: options.version,
		cli:     execCLI,
	}
	// Steps inherit both.
	os.Setenv(util.EnvKuberayVersion, run.version)
	os.Setenv(util.EnvChartVersion, strings.TrimPrefix(run.version, "v"))
	if err := run.preflight(ctx); err != nil {
		return err
	}
	if err := chartPublished(ctx, os.Getenv(util.EnvChartVersion)); err != nil {
		return err
	}

	byID, planned := util.ItemsByID(items), util.UnitsByID(units)
	rerun := len(ids) > 0
	if !rerun {
		for _, item := range items {
			ids = append(ids, item.ID)
		}
	}
	if err := os.MkdirAll(repoPaths.Results, 0o750); err != nil {
		return err
	}
	var todo []util.Unit
	for _, id := range ids {
		item, ok := byID[id]
		if !ok {
			return fmt.Errorf("no Item %s in items.yaml", id)
		}
		unit, ok := planned[id]
		var skip string
		switch {
		case len(item.Requires) > 0:
			skip = "requires " + strings.Join(item.Requires, ",")
		case !ok:
			skip = "not planned"
		}
		if skip != "" {
			if rerun {
				fmt.Printf("%s: skipped, %s\n", id, skip)
			}
			continue
		}
		if !rerun && done(filepath.Join(repoPaths.Results, id, util.ReportFile)) {
			continue
		}
		todo = append(todo, unit)
	}
	todo, err = run.fresh(ctx, byID, todo)
	if err != nil {
		return err
	}
	streak := 0 // units in a row that failed at their first step
	var broken error
	for _, unit := range todo {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fmt.Printf("%s ... ", unit.ID)
		failed, err := run.runUnit(ctx, byID[unit.ID], unit)
		if err != nil {
			return fmt.Errorf("%s: %w", unit.ID, err)
		}
		if failed != 1 {
			streak = 0
		} else if streak++; streak == 3 {
			broken = errors.New("3 units in a row failed at their first step: the environment is broken, not the release; see their reports, then run again")
			break
		}
	}
	// Also after a stop for a broken environment, so SUMMARY.md lists those failures.
	return errors.Join(report.WriteSummary(repoPaths, run.version), broken)
}

// runner executes units on the current kube context.
type runner struct {
	path    util.Paths
	version string
	cli     cliFunc // runs kubectl and helm
}

// chartPublished: a tag whose charts are not out yet, or a typo, would fail every unit at its
// first step.
func chartPublished(ctx context.Context, chartVersion string) error {
	body, _, found, err := source.FetchURL(ctx, chartIndex)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%s not found", chartIndex)
	}
	var index struct {
		Entries map[string][]struct {
			Version string `json:"version"`
		} `json:"entries"`
	}
	if err := yaml.Unmarshal(body, &index); err != nil {
		return fmt.Errorf("chart index: %w", err)
	}
	for _, entry := range index.Entries["kuberay-operator"] {
		if entry.Version == chartVersion {
			return nil
		}
	}
	return fmt.Errorf("chart kuberay-operator %s is not in %s; release.md step 6 publishes the charts", chartVersion, chartIndex)
}

// done: the unit has a result for this version. A stale report is not one; a report that does
// not parse is, so a plain run never destroys a triage someone wrote badly.
func done(reportPath string) bool {
	metadata, err := report.ReadFrontMatter(reportPath)
	if err != nil {
		_, err := os.Stat(filepath.Clean(reportPath))
		return err == nil
	}
	return metadata.Status != util.StatusStale
}

// fresh drops units whose source changed or vanished since planning and writes each a stale
// report: steps written for an older page would test the wrong thing. Needs the network.
func (r *runner) fresh(ctx context.Context, byID map[string]util.Item, units []util.Unit) ([]util.Unit, error) {
	if len(units) > 0 {
		fmt.Printf("checking the sources of %d units\n", len(units))
	}
	var fresh []util.Unit
	for _, unit := range units {
		src, err := source.Fetch(ctx, r.path, byID[unit.ID], r.version)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", unit.ID, err)
		}
		if src.Dead == "" && src.SHA == unit.SourceSHA {
			fresh = append(fresh, unit)
			continue
		}
		brief := fmt.Sprintf("source changed since the unit was planned (%.12s is now %.12s)", unit.SourceSHA, src.SHA)
		if src.Dead != "" {
			brief = "source is gone: " + src.Dead
		}
		dir := filepath.Join(r.path.Results, unit.ID)
		if err := os.RemoveAll(dir); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, err
		}
		stale := report.Render("stale.md.tmpl", report.FrontMatter{Item: unit.ID, Status: util.StatusStale, Version: r.version, Brief: brief})
		if err := os.WriteFile(filepath.Join(dir, util.ReportFile), []byte(stale), 0o600); err != nil {
			return nil, err
		}
		fmt.Printf("%s: stale, %s\n", unit.ID, brief)
	}
	return fresh, nil
}

// preflight checks that the current kube context is a kind cluster and is reachable.
func (r *runner) preflight(ctx context.Context) error {
	kctx, err := r.output(ctx, "kubectl", "config", "current-context")
	if err != nil {
		return errors.New("no current kube context")
	}
	if !strings.HasPrefix(kctx, "kind-") {
		return fmt.Errorf("context %s is not a kind cluster; the runner deletes what units leave behind", kctx)
	}
	if _, err := r.output(ctx, "kubectl", "get", "--raw", "/readyz"); err != nil {
		return fmt.Errorf("cluster %s is not reachable", kctx)
	}
	return nil
}
