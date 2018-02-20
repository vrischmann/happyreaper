package main

import (
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vrischmann/flagutil"
	"github.com/vrischmann/happyreaper/errors"
)

func makeURL(path string) string {
	host := flReaperHost[0]
	return "http://" + host + path
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
	rootCmd = &cobra.Command{
		Use:   "happyreaper [command]",
		Short: "communicate with Cassandra Reaper",
		Run: func(cmd *cobra.Command, args []string) {
			logrus.Warn("missing command")
			cmd.HelpFunc()(cmd, args)
		},
	}

	reaperHost   flagutil.NetworkAddresses
	flReaperHost = pflag.Flag{
		Name:      "host",
		Shorthand: "H",
		Value:     &reaperHost,
	}
)

func init() {
	rootCmd.PersistentFlags().AddFlag(&flReaperHost)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		logrus.Fatal(err)
	}
}
