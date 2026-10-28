package stale

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/release-test/pkg/util"
	"github.com/ray-project/kuberay/release-test/pkg/util/source"
)

var (
	staleLong = templates.LongDesc(`
		List the Items to plan, one line each: new (no unit yet), changed (the source changed since its
		unit was planned) or dead (the source is gone), and orphan for a unit without an Item. Items that
		require hardware are skipped. A docs page counts as changed only when its code blocks do:
		comment lines and KubeRay version references are ignored.

		Each docs page's markdown, each sample as it is at the tag under test, and each other URL's
		content is fetched into release-test/.cache/ for the release-test-plan skill to read. Prints
		nothing when the manifest is up to date.
	`)

	staleExample = templates.Examples(`
		# Every Item
		go run ./release-test stale --kuberay-version v1.8.0-rc.0

		# Only these Items
		go run ./release-test stale --kuberay-version v1.8.0-rc.0 raycluster-quick-start ray-cluster-sample
	`)
)

type staleOptions struct {
	paths   util.Paths
	version string
	ids     []string
}

func newStaleOptions() *staleOptions {
	return &staleOptions{}
}

func NewStaleCommand() *cobra.Command {
	options := newStaleOptions()

	cmd := &cobra.Command{
		Use:          "stale --kuberay-version TAG [ID...]",
		Short:        "List the Items whose manifest unit is missing or out of date",
		Long:         staleLong,
		Example:      staleExample,
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
	cmd.Flags().StringVar(&options.version, "kuberay-version", "", "the tag under test, e.g. v1.8.0-rc.0; samples are read as they are at this tag")
	cobra.CheckErr(cmd.MarkFlagRequired("kuberay-version"))

	return cmd
}

func (options *staleOptions) Complete(args []string) error {
	repoPaths, err := util.FindPaths()
	if err != nil {
		return err
	}
	options.paths = repoPaths
	options.ids = args
	return nil
}

func (options *staleOptions) Validate() error {
	return util.ValidateVersion(options.version)
}

// Run prints one line per Item whose unit is missing (new), outdated (changed) or whose source
// is gone (dead), and per unit without an Item (orphan).
func (options *staleOptions) Run(ctx context.Context) error {
	repoPaths, ids := options.paths, options.ids
	items, err := util.LoadItems(repoPaths.Items)
	if err != nil {
		return err
	}
	units, err := util.LoadManifest(repoPaths.Manifest)
	if err != nil {
		return err
	}
	byID, planned := util.ItemsByID(items), util.UnitsByID(units)
	if len(ids) == 0 {
		for _, unit := range units {
			if _, ok := byID[unit.ID]; !ok {
				fmt.Println("orphan", unit.ID)
			}
		}
		for _, item := range items {
			ids = append(ids, item.ID)
		}
	}
	for _, id := range ids {
		item, ok := byID[id]
		if !ok {
			return fmt.Errorf("no Item %s in items.yaml", id)
		}
		if len(item.Requires) > 0 {
			continue
		}
		src, err := source.Fetch(ctx, repoPaths, item, options.version)
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		unit, ok := planned[id]
		switch {
		case src.Dead != "":
			fmt.Println("dead", id, src.Dead)
		case !ok:
			fmt.Println("new", id, src.SHA, src.File)
		case unit.SourceSHA != src.SHA:
			fmt.Println("changed", id, src.SHA, src.File)
		}
	}
	return nil
}
