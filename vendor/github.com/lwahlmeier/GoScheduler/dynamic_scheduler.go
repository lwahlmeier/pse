package GoScheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

var MAX_WAIT_TIME = time.Minute

// DynamicSchduler is a scheduler that runs every job scheduled in
// its own go routine
type DynamicScheduler struct {
	clearChan chan bool
	jobs      []*job
	newJob    chan *job
	runJob    chan *job
	running   atomic.Bool
	waiter    sync.WaitGroup
}

func CreateDynamicScheduler() *DynamicScheduler {
	sch := &DynamicScheduler{
		clearChan: make(chan bool, 10),
		jobs:      make([]*job, 0),
		newJob:    make(chan *job),
		runJob:    make(chan *job),
		running:   atomic.Bool{},
		waiter:    sync.WaitGroup{},
	}
	sch.running.Store(true)
	go sch.run()
	return sch
}

func (sch *DynamicScheduler) doWork() {
	sch.waiter.Add(1)
	defer sch.waiter.Done()
	for sch.running.Load() {
		expireTimer := time.NewTimer(MAX_WAIT_TIME)
		select {
		case job := <-sch.runJob:
			expireTimer.Stop()
			select {
			case <-job.ctx.Done():
				continue
			default:
			}
			execJob(job, sch)
		case <-expireTimer.C:
			return
		}
	}
}

func (sch *DynamicScheduler) run() {
	sch.waiter.Add(1)
	defer sch.waiter.Done()
	baseDelay := time.After(waitTime)
	nextDelay := baseDelay
	needSort := false
	for sch.running.Load() {
		select {
		case <-sch.clearChan:
			sch.jobs = make([]*job, 0)
			nextDelay = baseDelay
		case <-nextDelay:
			nextDelay = baseDelay
			if len(sch.jobs) > 0 {
				runJob := sch.jobs[0]
				sch.jobs = sch.jobs[1:]

				select {
				case sch.runJob <- runJob:
				default:
					go sch.doWork()
					sch.runJob <- runJob
				}
				if len(sch.jobs) > 0 {
					if needSort {
						sortJobs(sch.jobs)
						needSort = false
					}
					nextDelay = sch.jobs[0].timer
				}
			} else {
				baseDelay = time.After(waitTime)
				nextDelay = baseDelay
			}

		case newJob := <-sch.newJob:
			if len(sch.jobs) == 0 {
				sch.jobs = append(sch.jobs, newJob)
				nextDelay = newJob.timer
			} else if sch.jobs[0].nextCall.After(newJob.nextCall) {
				sch.jobs = append([]*job{newJob}, sch.jobs...)
				nextDelay = newJob.timer
			} else {
				sch.jobs = append(sch.jobs, newJob)
				needSort = true
			}
		}
	}
}

func (sch *DynamicScheduler) Schedule(delay time.Duration, recurring bool, callable func()) {
	sch.ScheduleWithContext(delay, recurring, callable, context.Background())
}

func (sch *DynamicScheduler) ScheduleWithContext(delay time.Duration, recurring bool, callable func(), ctx context.Context) {
	if !sch.running.Load() {
		return
	}
	context.TODO()
	runTime := time.Now().Add(delay)
	sch.newJob <- &job{
		callable:  callable,
		nextCall:  runTime,
		timer:     time.After(time.Until(runTime)),
		period:    delay,
		recurring: recurring,
		ctx:       ctx,
	}
}

func (sch *DynamicScheduler) Stop() {
	if sch.running.CompareAndSwap(true, false) {
		sch.Clear()
		for {
			select {
			case sch.runJob <- &job{recurring: false, callable: func() {}, ctx: context.Background()}:
			default:
				return
			}
		}
	}
}

func (sch *DynamicScheduler) IsRunning() bool {
	return sch.running.Load()
}

func (sch *DynamicScheduler) Clear() {
	select {
	case sch.clearChan <- true:
	default:
	}
}

func (sch *DynamicScheduler) WaitForStop() {
	sch.Stop()
	sch.waiter.Wait()
}
