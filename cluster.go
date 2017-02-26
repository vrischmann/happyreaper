package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/fatih/color"
	"github.com/vrischmann/flagutil"
	"github.com/vrischmann/happyreaper/errors"
)

type Cluster struct {
	Name            string           `json:"name"`
	SeedHosts       []string         `json:"seed_hosts"`
	RepairRuns      []RepairRun      `json:"repair_runs"`
	RepairSchedules []RepairSchedule `json:"repair_schedules"`
}

func listClusters(args []string) error {
	const op = "listClusters"

	resp, err := http.Get(makeURL("/cluster"))
	if err != nil {
		return errors.E(errors.IO, op, err)
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	var res []string

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	fmt.Println("All clusters:\n")
	for _, cl := range res {
		fmt.Println(cl)
	}

	return nil
}

type printClusterParams struct {
	ShowRuns            bool
	ShowSchedules       bool
	FilterCFs           []string
	FilterRunState      RunState
	FilterScheduleState ScheduleState
}

func printCluster(c Cluster, params printClusterParams) {
	color.Yellow("Seeds:\n")
	for _, seed := range c.SeedHosts {
		fmt.Println(seed)
	}
	fmt.Println("")

	if params.ShowRuns {
		var headerPrinted bool

		for _, run := range c.RepairRuns {
			if len(params.FilterCFs) > 0 {
				if !contains(run.ColumnFamilies, params.FilterCFs) {
					continue
				}
			}

			if params.FilterRunState != "" && run.State != params.FilterRunState {
				continue
			}

			if !headerPrinted {
				color.Yellow("Runs:\n")
				headerPrinted = true
			}

			fmt.Printf("%+v\n", run)
		}
	}

	if params.ShowSchedules {
		var headerPrinted bool

		for _, sc := range c.RepairSchedules {
			if len(params.FilterCFs) > 0 {
				if !contains(sc.ColumnFamilies, params.FilterCFs) {
					continue
				}
			}

			if params.FilterScheduleState != "" && sc.State != params.FilterScheduleState {
				continue
			}

			if !headerPrinted {
				color.Yellow("Schedules:\n")
			}

			fmt.Printf("%+v\n", sc)
		}
	}
}

func viewCluster(args []string) error {
	const op = "viewCluster"

	var (
		fs              = flag.NewFlagSet("view-cluster", flag.ContinueOnError)
		flShowRuns      = fs.Bool("runs", true, "Show all runs from this cluster")
		flShowSchedules = fs.Bool("schedules", false, "Show all schedules from this cluster")
		flCFs           flagutil.Strings
		flRunState      RunState
		flScheduleState ScheduleState
	)

	fs.Var(&flCFs, "cf", "Filter by column families")
	fs.Var(&flRunState, "run-state", "Filter by run state")
	fs.Var(&flScheduleState, "schedule-state", "Filter by schedule state")

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if fs.NArg() < 1 {
		return errors.Str("please provide a cluster name")
	}

	flName := fs.Arg(0)

	resp, err := http.Get(makeURL("/cluster/" + flName))
	if err != nil {
		return errors.E(errors.IO, op, err)
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	rd := io.TeeReader(resp.Body, &buf)

	if resp.StatusCode != http.StatusOK {
		io.Copy(&buf, rd)
		return errors.Str(buf.String())
	}

	var res Cluster
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	params := printClusterParams{
		ShowRuns:            *flShowRuns,
		ShowSchedules:       *flShowSchedules,
		FilterCFs:           flCFs,
		FilterRunState:      flRunState,
		FilterScheduleState: flScheduleState,
	}

	fmt.Printf("Cluster %q:\n\n", flName)
	printCluster(res, params)

	return nil
}

func addCluster(args []string) error {
	const op = "addCluster"

	var (
		fs     = flag.NewFlagSet("add-cluster", flag.ContinueOnError)
		flSeed = fs.String("seed", "", "The seed host")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flSeed == "" {
		return errors.Str("please provide a seed host")
	}

	resp, err := http.Post(makeURL("/cluster?seedHost="+*flSeed), "application/json", nil)
	if err != nil {
		return errors.E(errors.IO, op, err)
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	rd := io.TeeReader(resp.Body, &buf)

	if resp.StatusCode != http.StatusOK {
		io.Copy(&buf, rd)
		return errors.Str(buf.String())
	}

	var res Cluster
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	color.Yellow("Cluster %s correctly added", res.Name)

	printCluster(res, printClusterParams{})

	return nil
}
