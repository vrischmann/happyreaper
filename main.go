package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/vrischmann/flagutil"
	"github.com/vrischmann/happyreaper/errors"
)

func makeURL(path string) string {
	return "http://" + flReaperHost[0] + path
}

type Parallelism string

func (p Parallelism) String() string { return string(p) }

func (p *Parallelism) Set(s string) error {
	switch {
	case strings.EqualFold(s, "sequential"):
		*p = Sequential
	case strings.EqualFold(s, "parallel"):
		*p = Parallel
	case strings.EqualFold(s, "datacenter_aware"):
		*p = DatacenterAware
	default:
		return errors.Errorf("invalid parallelism %q", s)
	}
	return nil
}

const (
	Sequential      Parallelism = "SEQUENTIAL"
	Parallel        Parallelism = "PARALLEL"
	DatacenterAware Parallelism = "DATACENTER_AWARE"
)

func contains(a, b []string) bool {
	m := make(map[string]struct{})
	for _, el := range a {
		m[el] = struct{}{}
	}

	for _, el := range b {
		if _, ok := m[el]; ok {
			return true
		}
	}

	return false
}

type myTime struct {
	time.Time
}

const myTimeLayout = `2006-01-02`

func (t *myTime) Set(s string) error {
	t2, err := time.Parse(myTimeLayout, s)
	if err != nil {
		return err
	}

	*t = myTime{t2}

	return nil
}

func (t myTime) String() string {
	return t.Format(myTimeLayout)
}

var (
	mainFs       = flag.NewFlagSet("main", flag.ContinueOnError)
	flReaperHost flagutil.NetworkAddresses
)

func printMainUsage(name string, fs *flag.FlagSet) {
	mainFs.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Usage of %s\n", name)
	fs.PrintDefaults()
}

func init() {
	mainFs.Var(&flReaperHost, "host", "The reaper host")
}

type commandFn func([]string) error

var commands = map[string]map[string]commandFn{
	"cluster": {
		"add-cluster":   addCluster,
		"view-cluster":  viewCluster,
		"list-clusters": listClusters,
	},
	"repair": {
		"add-repair":    addRepair,
		"view-repair":   viewRepair,
		"list-repairs":  listRepairs,
		"pause-repair":  pauseRepair,
		"resume-repair": resumeRepair,
		"delete-repair": deleteRepair,
	},
	"schedule": {
		"add-schedule":    addSchedule,
		"view-schedule":   viewSchedule,
		"list-schedules":  listSchedules,
		"pause-schedule":  pauseSchedule,
		"resume-schedule": resumeSchedule,
		"delete-schedule": deleteSchedule,
	},
}

func findCommand(name string) commandFn {
	for _, group := range commands {
		if fn, ok := group[name]; ok {
			return fn
		}
	}
	return nil
}

func printUsage() {
	fmt.Println("\nAvailable sub commands:")
	for groupName, group := range commands {
		fmt.Printf("\t%s\n", groupName)
		for command := range group {
			fmt.Printf("\t\t%s\n", command)
		}
	}
}

func main() {
	err := mainFs.Parse(os.Args[1:])
	switch {
	case err == flag.ErrHelp:
		printUsage()
		return
	case err != nil:
		log.Fatal(err)
		return
	}

	if len(flReaperHost) == 0 {
		val := os.Getenv("REAPER_HOST")
		if val != "" {
			if err := flReaperHost.Set(val); err != nil {
				log.Fatalf("REAPER_HOST value is invalid. err=%v", err)
			}
		}
	}

	if len(flReaperHost) == 0 {
		log.Println("please provide a reaper host")
		flag.PrintDefaults()
		printUsage()
		os.Exit(1)
	}

	if mainFs.NArg() < 1 {
		log.Println("please provide a sub command")
		flag.PrintDefaults()
		printUsage()
		os.Exit(1)
	}

	command := mainFs.Arg(0)
	fn := findCommand(command)

	if fn == nil {
		log.Fatalf("invalid command %q", command)
	}

	if err := fn(mainFs.Args()[1:]); err != nil {
		log.Fatal(err)
	}
}
