package report

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/ray-project/kuberay/release-test/pkg/util"
)

// WriteSummary writes repoPaths.Results/SUMMARY.md for version.
func WriteSummary(repoPaths util.Paths, version string) error {
	items, err := util.LoadItems(repoPaths.Items)
	if err != nil {
		return err
	}
	units, err := util.LoadManifest(repoPaths.Manifest)
	if err != nil {
		return err
	}
	summary, counts := renderSummary(repoPaths.Results, items, units, version, time.Now())
	if err := os.MkdirAll(repoPaths.Results, 0o750); err != nil {
		return err
	}
	path := filepath.Join(repoPaths.Results, util.SummaryFile)
	if err := os.WriteFile(path, []byte(summary), 0o600); err != nil {
		return err
	}
	fmt.Printf("wrote %s: %s\n", repoPaths.Rel(path), counts)
	return nil
}

// summaryData is what summary.md.tmpl writes into SUMMARY.md.
type summaryData struct {
	Version, Date, Counts string

	Fail, Unreadable, Stale, Manual, NotRun, Unplanned, Pass []summaryRow
}

// summaryRow is one Item in SUMMARY.md.
type summaryRow struct {
	util.Item
	Report FrontMatter
	Err    error
}

// renderSummary has one row per Item.
func renderSummary(results string, items []util.Item, units []util.Unit, version string, now time.Time) (summary, counts string) {
	planned := util.UnitsByID(units)
	var data summaryData

	for _, item := range items {
		_, ok := planned[item.ID]
		row := summaryRow{Item: item}
		row.Report, row.Err = ReadFrontMatter(filepath.Join(results, item.ID, util.ReportFile))

		switch {
		case row.Err == nil:
			switch row.Report.Status {
			case util.StatusPass:
				data.Pass = append(data.Pass, row)
			case util.StatusStale:
				data.Stale = append(data.Stale, row)
			default:
				data.Fail = append(data.Fail, row)
			}
		case !errors.Is(row.Err, fs.ErrNotExist):
			data.Unreadable = append(data.Unreadable, row)
		case len(item.Requires) > 0:
			data.Manual = append(data.Manual, row)
		case !ok:
			data.Unplanned = append(data.Unplanned, row)
		default:
			data.NotRun = append(data.NotRun, row)
		}
	}

	counts = fmt.Sprintf("%d pass, %d fail, %d stale, %d manual, %d not run, %d not planned",
		len(data.Pass), len(data.Fail), len(data.Stale), len(data.Manual), len(data.NotRun), len(data.Unplanned))

	if len(data.Unreadable) > 0 {
		counts += fmt.Sprintf(", %d unreadable", len(data.Unreadable))
	}

	data.Version, data.Date, data.Counts = version, now.UTC().Format(time.DateOnly), counts
	return Render("summary.md.tmpl", data), counts
}
