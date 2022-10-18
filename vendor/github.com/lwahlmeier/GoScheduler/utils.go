package GoScheduler

import (
	"context"
	"sort"
	"time"
)

type job struct {
	callable  func()
	nextCall  time.Time
	timer     <-chan time.Time
	period    time.Duration
	recurring bool
	ctx       context.Context
}

func (j job) String() string {
	return j.period.String()
}

type jobSort []*job

func (js jobSort) Len() int {
	return len(js)
}

func (js jobSort) Swap(i, j int) {
	js[i], js[j] = js[j], js[i]
}

func (js jobSort) Less(i, j int) bool {
	return js[i].nextCall.Before(js[j].nextCall)
}

func execJob(rj *job, sch Scheduler) {
	rj.callable()
	if rj.recurring {
		sch.ScheduleWithContext(rj.period, true, rj.callable, rj.ctx)
	}
}

func sortJobs(jobs []*job) time.Duration {
	cl := len(jobs)
	if cl == 0 {
		return waitTime
	}
	if cl > 1 {
		sort.Sort(jobSort(jobs))
	}
	return time.Until(jobs[0].nextCall)
}
