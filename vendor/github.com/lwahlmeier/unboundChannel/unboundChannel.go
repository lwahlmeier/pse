package unboundchannel

import (
	"sync/atomic"
)

type UnboundChannel[T any] struct {
	data         []T
	inChannel    chan T
	outChannel   chan T
	countChannel chan int
	running      atomic.Bool
}

func NewUnboundChannel[T any]() *UnboundChannel[T] {
	uc := &UnboundChannel[T]{
		data:         make([]T, 0),
		inChannel:    make(chan T, 0),
		outChannel:   make(chan T, 0),
		countChannel: make(chan int),
		running:      atomic.Bool{},
	}
	uc.running.Store(true)
	go uc.run()
	return uc
}

func (uc *UnboundChannel[T]) Add() chan<- T {
	return uc.inChannel
}

func (uc *UnboundChannel[T]) Get() <-chan T {
	return uc.outChannel
}

func (uc *UnboundChannel[T]) Count() <-chan int {
	return uc.countChannel
}

func (uc *UnboundChannel[T]) IsRunning() bool {
	return uc.running.Load()
}

func (uc *UnboundChannel[T]) Stop() {
	if uc.running.CompareAndSwap(true, false) {
		select {
		case <-uc.Count():
		default:
		}
		close(uc.countChannel)
		close(uc.outChannel)
		close(uc.inChannel)
		uc.data = nil
	}
}

func (uc *UnboundChannel[T]) run() {
	for uc.running.Load() {
		if len(uc.data) > 0 {
			select {
			case newMsg := <-uc.inChannel:
				uc.data = append(uc.data, newMsg)
			case uc.outChannel <- uc.data[0]:
				uc.data = uc.data[1:]
			case uc.countChannel <- len(uc.data) + len(uc.outChannel):
			}
		} else {
			select {
			case newStr := <-uc.inChannel:
				uc.data = append(uc.data, newStr)
			case uc.countChannel <- len(uc.data) + len(uc.outChannel):
			}
		}
	}
}
