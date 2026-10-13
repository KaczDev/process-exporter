package collector

import (
	"strings"
	"testing"

	common "github.com/ncabatoff/process-exporter"
	"github.com/ncabatoff/process-exporter/proc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type testSource struct {
	procs []proc.IDInfo
}

func (s *testSource) AllProcs() proc.Iter {
	return &testIter{procs: s.procs}
}

type testIter struct {
	proc.IDInfo
	procs []proc.IDInfo
}

func (it *testIter) Next() bool {
	if len(it.procs) == 0 {
		return false
	}
	it.IDInfo = it.procs[0]
	it.procs = it.procs[1:]
	return true
}

func (it *testIter) Close() error { return nil }

type testNamer struct{}

func (testNamer) MatchAndName(common.ProcAttributes) (bool, string) { return true, "group" }
func (testNamer) String() string                                    { return "group" }

type synchronousCollector struct {
	*NamedProcessCollector
}

func (p synchronousCollector) Collect(ch chan<- prometheus.Metric) { p.scrape(ch) }

func TestIOOpsMetrics(t *testing.T) {
	for _, threads := range []bool{false, true} {
		name := "without threads"
		if threads {
			name = "with threads"
		}
		t.Run(name, func(t *testing.T) {
			info := proc.IDInfo{
				ID: proc.ID{Pid: 1},
				Metrics: proc.Metrics{
					Counts:   proc.Counts{ReadOps: 100, WriteOps: 200},
					Filedesc: proc.Filedesc{Open: 1, Limit: 10},
				},
				Threads: []proc.Thread{
					{ThreadID: proc.ThreadID{Pid: 1}, ThreadName: "worker",
						Counts: proc.Counts{ReadOps: 40, WriteOps: 80}},
					{ThreadID: proc.ThreadID{Pid: 2}, ThreadName: "worker",
						Counts: proc.Counts{ReadOps: 60, WriteOps: 120}},
				},
			}
			source := &testSource{procs: []proc.IDInfo{info}}
			p := &NamedProcessCollector{
				Grouper: proc.NewGrouper(testNamer{}, false, threads, false, 0, false, false),
				source:  source,
				threads: threads,
			}
			if _, _, err := p.Update(source.AllProcs()); err != nil {
				t.Fatal(err)
			}
			source.procs[0].ReadOps += 7
			source.procs[0].WriteOps += 11
			source.procs[0].Threads[0].ReadOps += 3
			source.procs[0].Threads[0].WriteOps += 5
			source.procs[0].Threads[1].ReadOps += 4
			source.procs[0].Threads[1].WriteOps += 6

			registry := prometheus.NewPedanticRegistry()
			registry.MustRegister(synchronousCollector{p})
			want := `
# HELP namedprocess_namegroup_read_ops_total number of read I/O operations by this group based on /proc/[pid]/io syscr; not a direct measure of physical disk operations, since cached I/O can also count
# TYPE namedprocess_namegroup_read_ops_total counter
namedprocess_namegroup_read_ops_total{groupname="group"} 7
# HELP namedprocess_namegroup_write_ops_total number of write I/O operations by this group based on /proc/[pid]/io syscw; not a direct measure of physical disk operations, since cached I/O can also count
# TYPE namedprocess_namegroup_write_ops_total counter
namedprocess_namegroup_write_ops_total{groupname="group"} 11
`
			if threads {
				want += `
# HELP namedprocess_namegroup_thread_io_ops_total number of read/write I/O operations by these threads based on /proc/[pid]/task/[tid]/io syscr/syscw; not a direct measure of physical disk operations, since cached I/O can also count
# TYPE namedprocess_namegroup_thread_io_ops_total counter
namedprocess_namegroup_thread_io_ops_total{groupname="group",iomode="read",threadname="worker"} 7
namedprocess_namegroup_thread_io_ops_total{groupname="group",iomode="write",threadname="worker"} 11
`
			}
			if err := testutil.GatherAndCompare(registry, strings.NewReader(want),
				"namedprocess_namegroup_read_ops_total",
				"namedprocess_namegroup_write_ops_total",
				"namedprocess_namegroup_thread_io_ops_total"); err != nil {
				t.Fatal(err)
			}
		})
	}
}
