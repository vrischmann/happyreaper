package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/vrischmann/flagutil"
	"github.com/vrischmann/happyreaper/errors"
)

type ScheduleState string

const (
	SActive  ScheduleState = "ACTIVE"
	SPaused  ScheduleState = "PAUSED"
	SDeleted ScheduleState = "DELETED"
)

func (s ScheduleState) String() string { return string(s) }

func (s *ScheduleState) Set(str string) error {
	switch {
	case strings.EqualFold(str, "active"):
		*s = SActive
	case strings.EqualFold(str, "paused"):
		*s = SPaused
	case strings.EqualFold(str, "deleted"):
		*s = SDeleted
	default:
		return errors.Errorf("invalid state %q", str)
	}
	return nil
}

type RepairSchedule struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`

	ClusterName  string `json:"cluster_name"`
	KeyspaceName string `json:"keyspace_name"`

	State ScheduleState `json:"state"`

	ColumnFamilies       []string    `json:"column_families"`
	Intensity            float64     `json:"intensity"`
	IncrementalRepair    bool        `json:"incremental_repair"`
	RepairParallelism    Parallelism `json:"repair_parallelism"`
	ScheduledDaysBetween int         `json:"scheduled_days_between"`
	SegmentCount         int         `json:"segment_count"`

	CreationTime   *time.Time `json:"creation_time"`
	PauseTime      *time.Time `json:"pause_time"`
	NextActivation *time.Time `json:"next_activation"`
}

func (r RepairSchedule) String() string {
	s := fmt.Sprintf("{id:%d owner:%q cluster:%q keyspace:%q state:%s cf:%v intensity:%0.3f par:%s daysBetween:%d segments:%d creation:%s pause:%s next:%s}",
		r.ID, r.Owner,
		r.ClusterName, r.KeyspaceName,
		r.State, r.ColumnFamilies,
		r.Intensity, r.RepairParallelism,
		r.ScheduledDaysBetween, r.SegmentCount,
		r.CreationTime, r.PauseTime, r.NextActivation,
	)
	return s
}

func (r RepairSchedule) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%-20s %s\n", "id:", r.ID)
			fmt.Fprintf(s, "%-20s %s\n", "owner:", r.Owner)
			fmt.Fprintf(s, "%-20s %s\n", "cluster name:", r.ClusterName)
			fmt.Fprintf(s, "%-20s %s\n", "keyspace name:", r.KeyspaceName)
			fmt.Fprintf(s, "%-20s %s\n", "state:", r.State)
			fmt.Fprintf(s, "%-20s %v\n", "column families:", r.ColumnFamilies)
			fmt.Fprintf(s, "%-20s %0.3f\n", "intensity:", r.Intensity)
			fmt.Fprintf(s, "%-20s %s\n", "par:", r.RepairParallelism)
			fmt.Fprintf(s, "%-20s %d\n", "days between:", r.ScheduledDaysBetween)
			fmt.Fprintf(s, "%-20s %d\n", "segments:", r.SegmentCount)
			fmt.Fprintf(s, "%-20s %s\n", "creation time:", r.CreationTime)
			fmt.Fprintf(s, "%-20s %s\n", "pause time:", r.PauseTime)
			fmt.Fprintf(s, "%-20s %s\n", "next activation:", r.NextActivation)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, r.String())
	}
}

func addSchedule(args []string) error {
	const op = "addSchedule"

	var (
		fs                    = flag.NewFlagSet("add-schedule", flag.ContinueOnError)
		flCluster             = fs.String("cluster", "", "The cluster name")
		flKeyspace            = fs.String("keyspace", "", "The keyspace name")
		flTables              flagutil.Strings
		flOwner               = fs.String("owner", "", "The owner")
		flSegments            = fs.Int("segments", 200, "The number of segments")
		flPar                 Parallelism
		flIntensity           = fs.Float64("intensity", 0.5, "The intensity")
		flIncrementalRepair   = fs.Bool("incremental", false, "Use incremental repairs")
		flScheduleDaysBetween = fs.Int("schedule-days-between", 14, "Number of days between repairs")
		flScheduleTriggerTime = fs.String("schedule-trigger-time", "", "Time at which to start the scheduling")
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

	if *flIncrementalRepair {
		fmt.Println("NOTE: incremental repairs are not supported yet")
	}

	switch {
	case *flCluster == "":
		return errors.Str("please provide a cluster")
	case *flKeyspace == "":
		return errors.Str("please provide a keyspace")
	case *flOwner == "":
		return errors.Str("please provide an owner")
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
	qry.Add("segmentCount", strconv.Itoa(*flSegments))
	qry.Add("repairParallelism", flPar.String())
	qry.Add("intensity", fmt.Sprintf("%0.3f", *flIntensity))
	qry.Add("scheduleDaysBetween", strconv.Itoa(*flScheduleDaysBetween))
	if *flScheduleTriggerTime != "" {
		qry.Add("scheduleTriggerTime", *flScheduleTriggerTime)
	}

	ur := makeURL("/repair_schedule?") + qry.Encode()

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

	var res RepairSchedule
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	color.Yellow("Schedule #%d correctly added", res.ID)

	fmt.Printf("%+v\n", res)

	return nil
}

func viewSchedule(args []string) error {
	const op = "viewSchedule"

	var (
		fs   = flag.NewFlagSet("view-schedule", flag.ContinueOnError)
		flID = fs.String("id", "", "The repair ID")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flID == "" {
		return errors.Str("please provide a valid ID")
	}

	resp, err := http.Get(makeURL("/repair_schedule/" + *flID))
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

	var res RepairSchedule
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	fmt.Printf("%+v\n", res)

	return nil
}

type ScheduleSortBy string

const (
	ScheduleSortByNextActivation = "next-activation"
)

func (s ScheduleSortBy) String() string { return string(s) }
func (s *ScheduleSortBy) Set(str string) error {
	*s = ScheduleSortBy(str)
	return nil
}

func callListSchedules(qry url.Values) ([]RepairSchedule, error) {
	const op = "callListSchedules"

	resp, err := http.Get(makeURL("/repair_schedule") + "?" + qry.Encode())
	if err != nil {
		return nil, errors.E(errors.IO, op, err)
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	rd := io.TeeReader(resp.Body, &buf)

	if resp.StatusCode != http.StatusOK {
		io.Copy(&buf, rd)
		return nil, errors.Str(buf.String())
	}

	var res []RepairSchedule
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return nil, errors.E(errors.IO, op, err)
	}
	return res, nil
}

func sortSchedules(res []RepairSchedule, sortBy ScheduleSortBy, reverse bool) {
	switch {
	case sortBy == ScheduleSortByNextActivation && !reverse:
		sort.Slice(res, func(i, j int) bool {
			return res[i].NextActivation.Before(*res[j].NextActivation)
		})
	case sortBy == ScheduleSortByNextActivation:
		sort.Slice(res, func(i, j int) bool {
			return res[i].NextActivation.After(*res[j].NextActivation)
		})
	}
}

func nextSchedule(args []string) error {
	const op = "nextSchedule"

	var fs = flag.NewFlagSet("next-schedule", flag.ContinueOnError)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	res, err := callListSchedules(make(url.Values))
	if err != nil {
		return err
	}

	if len(res) <= 0 {
		return nil
	}

	sortSchedules(res, ScheduleSortByNextActivation, false)

	schedule := res[0]
	fmt.Printf("%+v\n\n", schedule)

	return nil
}

func listSchedules(args []string) error {
	const op = "listSchedules"

	var (
		fs            = flag.NewFlagSet("list-schedules", flag.ContinueOnError)
		flCluster     = fs.String("cluster", "", "The cluster name")
		flKeyspace    = fs.String("keyspace", "", "The keyspace name")
		flState       ScheduleState
		flSortBy      ScheduleSortBy
		flReverseSort = fs.Bool("reverse-sort", false, "Revert the sorting")
	)

	fs.Var(&flState, "state", "Filter by state")
	fs.Var(&flSortBy, "sort-by", "Sort by next-activation")

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	qry := make(url.Values)
	if *flCluster != "" {
		qry.Add("clusterName", *flCluster)
	}
	if *flKeyspace != "" {
		qry.Add("keyspaceName", *flKeyspace)
	}

	res, err := callListSchedules(qry)
	if err != nil {
		return err
	}

	sortSchedules(res, flSortBy, *flReverseSort)

	for _, sched := range res {
		keyspaceOK := *flKeyspace == "" || sched.KeyspaceName == *flKeyspace
		stateOK := flState == "" || sched.State == flState

		if !keyspaceOK || !stateOK {
			continue
		}

		fmt.Printf("%+v\n\n", sched)
	}

	return nil
}

func deleteSchedule(args []string) error {
	const op = "deleteSchedule"

	var (
		fs      = flag.NewFlagSet("delete-schedule", flag.ContinueOnError)
		flID    = fs.String("id", "", "The schedule ID")
		flOwner = fs.String("owner", "", "The owner")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flID == "" {
		return errors.Str("please provide a valid ID")
	}
	if *flOwner == "" {
		return errors.Str("please provide a valid owner")
	}

	qry := make(url.Values)
	qry.Add("owner", *flOwner)

	ur := makeURL("/repair_schedule/"+*flID) + "?" + qry.Encode()

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

	var res RepairSchedule
	dec := json.NewDecoder(rd)

	if err := dec.Decode(&res); err != nil {
		return errors.E(errors.IO, op, err)
	}

	color.Yellow("Schedule %s correctly deleted", *flID)

	fmt.Printf("%+v\n", res)

	return nil
}

func changeScheduleState(repairID string, state ScheduleState) error {
	const op = "changeScheduleState"

	qry := make(url.Values)
	qry.Add("state", state.String())

	ur := makeURL("/repair_schedule/"+repairID) + "?" + qry.Encode()

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

func pauseSchedule(args []string) error {
	var (
		fs   = flag.NewFlagSet("pause-schedule", flag.ContinueOnError)
		flID = fs.String("id", "", "The schedule ID")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flID == "" {
		return errors.Str("please provide a valid ID")
	}

	return changeScheduleState(*flID, SPaused)
}

func resumeSchedule(args []string) error {
	var (
		fs   = flag.NewFlagSet("resume-schedule", flag.ContinueOnError)
		flID = fs.String("id", "", "The schedule ID")
	)

	err := fs.Parse(args)
	switch {
	case err == flag.ErrHelp:
		return nil
	case err != nil:
		return err
	}

	if *flID == "" {
		return errors.Str("please provide a valid ID")
	}

	return changeScheduleState(*flID, SActive)
}
