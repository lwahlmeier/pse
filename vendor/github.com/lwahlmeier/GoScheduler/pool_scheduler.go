package GoScheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type PoolScheduler struct {
	runJob    chan *job
	clearChan chan bool
	jobs      []*job
	newJob    chan *job
	running   atomic.Bool
	workers   int
	waiter    sync.WaitGroup
}

func CreatePoolScheduler(workers int) *PoolScheduler {
	ps := &PoolScheduler{
		clearChan: make(chan bool, 10),
		newJob:    make(chan *job),
		runJob:    make(chan *job, 1),
		jobs:      make([]*job, 0),
		running:   atomic.Bool{},
		workers:   workers,
		waiter:    sync.WaitGroup{},
	}
	ps.running.Store(true)
	for i := 0; i < workers; i++ {
		go ps.doWork()
	}
	go ps.run()
	return ps
}

func (ps *PoolScheduler) doWork() {
	ps.waiter.Add(1)
	defer ps.waiter.Done()
	for ps.running.Load() {
		job := <-ps.runJob
		select {
		case <-job.ctx.Done():
			continue
		default:
		}
		execJob(job, ps)
	}
}

func (ps *PoolScheduler) run() {
	ps.waiter.Add(1)
	defer ps.waiter.Done()
	baseDelay := time.After(waitTime)
	nextDelay := baseDelay
	needSort := false
	for ps.running.Load() {
		select {
		case <-ps.clearChan:
			ps.jobs = make([]*job, 0)
			nextDelay = baseDelay
		case <-nextDelay:
			nextDelay = baseDelay
			if len(ps.jobs) > 0 {
				runJob := ps.jobs[0]
				ps.jobs = ps.jobs[1:]

				ps.runJob <- runJob
				if len(ps.jobs) > 0 {
					if needSort {
						sortJobs(ps.jobs)
						needSort = false
					}
					nextDelay = ps.jobs[0].timer
				}
			} else {
				baseDelay = time.After(waitTime)
				nextDelay = baseDelay
			}
		case newJob := <-ps.newJob:
			if len(ps.jobs) == 0 {
				ps.jobs = append(ps.jobs, newJob)
				nextDelay = newJob.timer
			} else if ps.jobs[0].nextCall.After(newJob.nextCall) {
				ps.jobs = append([]*job{newJob}, ps.jobs...)
				nextDelay = newJob.timer
			} else {
				ps.jobs = append(ps.jobs, newJob)
				needSort = true
			}
		}
	}
}

func (ps *PoolScheduler) Schedule(delay time.Duration, recurring bool, callable func()) {
	ps.ScheduleWithContext(delay, recurring, callable, context.Background())
}

func (ps *PoolScheduler) ScheduleWithContext(delay time.Duration, recurring bool, callable func(), ctx context.Context) {
	if !ps.running.Load() {
		return
	}
	runTime := time.Now().Add(delay)
	toRun := &job{
		callable:  callable,
		nextCall:  runTime,
		timer:     time.After(time.Until(runTime)),
		period:    delay,
		recurring: recurring,
		ctx:       ctx,
	}
	ps.newJob <- toRun
}

func (ps *PoolScheduler) IsRunning() bool {
	return ps.running.Load()
}

func (ps *PoolScheduler) Stop() {
	if ps.running.CompareAndSwap(true, false) {
		ps.Clear()
		for i := 0; i < ps.workers; i++ {
			select {
			case ps.runJob <- &job{recurring: false, callable: func() {}, ctx: context.Background()}:
			default:
			}
		}
	}
}

func (ps *PoolScheduler) Clear() {
	select {
	case ps.clearChan <- true:
	default:
	}
}

func (ps *PoolScheduler) WaitForStop() {
	ps.Stop()
	ps.waiter.Wait()
}
