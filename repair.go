package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/vrischmann/flagutil"
	"github.com/vrischmann/happyreaper/errors"
)

type RunState string

const (
	NotStarted RunState = "NOT_STARTED"
	Running    RunState = "RUNNING"
	Error      RunState = "ERROR"
	Done       RunState = "DONE"
	Paused     RunState = "PAUSED"
	Aborted    RunState = "ABORTED"
	Deleted    RunState = "DELETED"
)

func (s RunState) String() string { return string(s) }

func (s *RunState) Set(str string) error {
	switch {
	case strings.EqualFold(str, "not_started"):
		*s = NotStarted
	case strings.EqualFold(str, "running"):
		*s = Running
	case strings.EqualFold(str, "error"):
		*s = Error
	case strings.EqualFold(str, "done"):
		*s = Done
	case strings.EqualFold(str, "paused"):
		*s = Paused
	case strings.EqualFold(str, "aborted"):
		*s = Aborted
	case strings.EqualFold(str, "deleted"):
		*s = Deleted
	default:
		return errors.Errorf("invalid state %q", str)
	}
	return nil
}

type RepairRun struct {
	ID    int    `json:"id"`
	Owner string `json:"owner"`

	ClusterName  string `json:"cluster_name"`
	KeyspaceName string `json:"keyspace_name"`

	State RunState `json:"state"`

	Cause            string   `json:"cause"`
	ColumnFamilies   []string `json:"column_families"`
	Intensity        float64  `json:"intensity"`
	TotalSegments    int      `json:"total_segments"`
	SegmentsRepaired int      `json:"segments_repaired"`
	LastEvent        string   `json:"last_event"`
	Duration         string   `json:"duration"`

	CreationTime *time.Time `json:"creation_time"`
	StartTime    *time.Time `json:"start_time"`
	EndTime      *time.Time `json:"end_time"`
	PauseTime    *time.Time `json:"pause_time"`
}

func (r RepairRun) String() string {
	s := fmt.Sprintf("{id:%d owner:%q cluster:%q keyspace:%q state:%s cause:%q cf:%v intensity:%0.3f segments:%d repaired:%d lastEvent:%q duration:%q creation:%s start:%s end:%s pause:%d}",
		r.ID, r.Owner,
		r.ClusterName, r.KeyspaceName,
		r.State, r.Cause,
		r.ColumnFamilies, r.Intensity,
		r.TotalSegments, r.SegmentsRepaired,
		r.LastEvent, r.Duration,
		r.CreationTime, r.StartTime,
		r.EndTime, r.PauseTime,
	)
	return s
}

func (r RepairRun) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%-20s %d\n", "id:", r.ID)
			fmt.Fprintf(s, "%-20s %s\n", "owner:", r.Owner)
			fmt.Fprintf(s, "%-20s %s\n", "cluster name:", r.ClusterName)
			fmt.Fprintf(s, "%-20s %s\n", "keyspace name:", r.KeyspaceName)
			fmt.Fprintf(s, "%-20s %s\n", "state:", r.State)
			fmt.Fprintf(s, "%-20s %s\n", "cause:", r.Cause)
			fmt.Fprintf(s, "%-20s %v\n", "column families:", r.ColumnFamilies)
			fmt.Fprintf(s, "%-20s %0.3f\n", "intensity:", r.Intensity)
			fmt.Fprintf(s, "%-20s %d\n", "total segments:", r.TotalSegments)
			fmt.Fprintf(s, "%-20s %d\n", "segments repaired:", r.SegmentsRepaired)
			fmt.Fprintf(s, "%-20s %s\n", "last event:", r.LastEvent)
			fmt.Fprintf(s, "%-20s %s\n", "duration:", r.Duration)
			fmt.Fprintf(s, "%-20s %s\n", "creation time:", r.CreationTime)
			fmt.Fprintf(s, "%-20s %s\n", "start time:", r.StartTime)
			fmt.Fprintf(s, "%-20s %s\n", "end time:", r.EndTime)
			fmt.Fprintf(s, "%-20s %s\n", "pause time:", r.PauseTime)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, r.String())
	}
}

func listRepairs(args []string) error {
	const op = "listRepairs"

	var (
		fs            = flag.NewFlagSet("list-repairs", flag.ContinueOnError)
		flRunState    RunState
		flCluster     = fs.String("cluster", "", "Filter by cluster")
		flKeyspace    = fs.String("keyspace", "", "Filter by keyspace")
		flTables      flagutil.Strings
		flOwner       = fs.String("owner", "", "Filter by owner")
		flCause       = fs.String("cause", "", "Filter by cause")
		flStartAfter  myTime
		flStartBefore myTime
	)

	fs.Var(&flRunState, "run-state", "Filter by run state")
	fs.Var(&flTables, "tables", "Filter by tables (comma separated list of tables)")
	fs.Var(&flStartAfter, "start-after", "Filter by runs that start after this date")
	fs.Var(&flStartBefore, "start-before", "Filter by runs that start before this date")

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	qry := make(url.Values)
	if flRunState != "" {
		qry.Add("state", flRunState.String())
	}

	resp, err := http.Get(makeURL("/repair_run?") + qry.Encode())
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

	var res []RepairRun
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	for _, run := range res {
		switch {
		case *flCluster != "" && *flCluster != run.ClusterName:
			continue

		case *flKeyspace != "" && *flKeyspace != run.KeyspaceName:
			continue

		case len(flTables) > 0 && contains(run.ColumnFamilies, flTables):
			continue

		case *flOwner != "" && *flOwner != run.Owner:
			continue

		case *flCause != "" && *flCause != run.Cause:
			continue

		case (!flStartAfter.IsZero() || !flStartBefore.IsZero()) && run.StartTime == nil:
			continue

		case !flStartAfter.IsZero() && run.StartTime.Before(flStartAfter.Time):
			continue

		case !flStartBefore.IsZero() && run.StartTime.After(flStartBefore.Time):
			continue
		}

		fmt.Printf("%+v\n", run)
	}

	return nil
}

func viewRepair(args []string) error {
	const op = "viewRepair"

	var (
		fs   = flag.NewFlagSet("view-repair", flag.ContinueOnError)
		flID = fs.Int("id", -1, "The repair ID")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flID <= 0 {
		return errors.Str("please provide a valid ID")
	}

	resp, err := http.Get(makeURL("/repair_run/" + strconv.Itoa(*flID)))
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

	var res RepairRun
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	fmt.Printf("%+v\n", res)

	return nil
}

func changeRepairState(repairID int, state RunState) error {
	const op = "changeRepairState"

	qry := make(url.Values)
	qry.Add("state", state.String())

	ur := makeURL("/repair_run/"+strconv.Itoa(repairID)) + "?" + qry.Encode()

	req, err := http.NewRequest("PUT", ur, nil)
	if err != nil {
		return errors.E(errors.Invalid, op, err)
	}

	resp, err := http.DefaultClient.Do(req)
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

	color.Yellow("State changed to %s", state)

	if buf.Len() > 0 {
		fmt.Println(buf.String())
	}

	return nil
}

func pauseRepair(args []string) error {
	var (
		fs   = flag.NewFlagSet("pause-repair", flag.ContinueOnError)
		flID = fs.Int("id", -1, "The repair ID")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flID <= 0 {
		return errors.Str("please provide a valid ID")
	}

	return changeRepairState(*flID, Paused)
}

func resumeRepair(args []string) error {
	var (
		fs   = flag.NewFlagSet("resume-repair", flag.ContinueOnError)
		flID = fs.Int("id", -1, "The repair ID")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flID <= 0 {
		return errors.Str("please provide a valid ID")
	}

	return changeRepairState(*flID, Running)
}

func deleteRepair(args []string) error {
	const op = "deleteRepair"

	var (
		fs      = flag.NewFlagSet("delete-repair", flag.ContinueOnError)
		flID    = fs.Int("id", -1, "The repair ID")
		flOwner = fs.String("owner", "", "The owner")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flID <= 0 {
		return errors.Str("please provide a valid ID")
	}
	if *flOwner == "" {
		return errors.Str("please provide a valid owner")
	}

	qry := make(url.Values)
	qry.Add("owner", *flOwner)

	ur := makeURL("/repair_run/" + strconv.Itoa(*flID) + "?" + qry.Encode())

	req, err := http.NewRequest("DELETE", ur, nil)
	if err != nil {
		return errors.E(errors.Invalid, op, err)
	}

	resp, err := http.DefaultClient.Do(req)
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

	fmt.Println(buf.String())

	return nil
}

func addRepair(args []string) error {
	const op = "addRepair"

	var (
		fs          = flag.NewFlagSet("add-repair", flag.ContinueOnError)
		flCluster   = fs.String("cluster", "", "The cluster name")
		flKeyspace  = fs.String("keyspace", "", "The keyspace name")
		flTables    flagutil.Strings
		flOwner     = fs.String("owner", "", "The owner")
		flCause     = fs.String("cause", "", "The cause for the repair")
		flSegments  = fs.Int("segments", 200, "The number of segments")
		flPar       Parallelism
		flIntensity = fs.Float64("intensity", 0.5, "The intensity")
	)

	fs.Var(&flTables, "tables", "The tables to repair")
	fs.Var(&flPar, "par", "The parallelism to use (default SEQUENTIAL)")

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	switch {
	case *flCluster == "":
		return errors.Str("please provide a cluster")
	case *flKeyspace == "":
		return errors.Str("please provide a keyspace")
	case *flOwner == "":
		return errors.Str("please provide an owner")
	case *flCause == "":
		return errors.Str("please provide a cause")
	}

	if flPar == "" {
		flPar = Sequential
	}

	qry := make(url.Values)
	qry.Add("clusterName", *flCluster)
	qry.Add("keyspace", *flKeyspace)
	if len(flTables) > 0 {
		qry.Add("tables", strings.Join(flTables, ","))
	}
	qry.Add("owner", *flOwner)
	qry.Add("cause", *flCause)
	qry.Add("segmentCount", strconv.Itoa(*flSegments))
	qry.Add("repairParallelism", flPar.String())
	qry.Add("intensity", fmt.Sprintf("%0.3f", *flIntensity))

	ur := makeURL("/repair_run?") + qry.Encode()

	resp, err := http.Post(ur, "application/json", nil)
	if err != nil {
		return errors.E(errors.IO, op, err)
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	rd := io.TeeReader(resp.Body, &buf)

	if resp.StatusCode != http.StatusCreated {
		io.Copy(&buf, rd)
		return errors.Str(buf.String())
	}

	var res RepairRun
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	color.Yellow("Repair #%d correctly added", res.ID)

	fmt.Printf("%+v\n", res)

	color.Yellow("NOTE: remember to resume-repair the repair just created\n")

	return nil
}
