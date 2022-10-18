package GoScheduler

import (
	"context"
	"sync/atomic"
	"time"
)

var defaultScheduler atomic.Pointer[DynamicScheduler]
var waitTime time.Duration = time.Hour * 1000

// Basic Schduler interface
type Scheduler interface {
	Clear()
	Stop()
	WaitForStop()
	IsRunning() bool
	Schedule(delay time.Duration, recurring bool, callable func())
	ScheduleWithContext(delay time.Duration, recurring bool, callable func(), ctx context.Context)
}

// Returns the global default scheduler(creates it if it doesnt exist)
// This is a DynamicSchduler
func GetDefaultScheduler() Scheduler {
	ds := defaultScheduler.Load()
	if ds != nil && ds.IsRunning() {
		return ds
	}
	nds := CreateDynamicScheduler()
	if defaultScheduler.CompareAndSwap(ds, nds) {
		return nds
	}
	nds.Stop()
	return defaultScheduler.Load()
}
