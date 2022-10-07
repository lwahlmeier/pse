package utils

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/pubsub/v1"
)

func TestDynamicUUIDChannel(t *testing.T) {
	count := int(math.Max(400, float64(rand.Intn(1000))))
	duc := NewDynamicUUIDChannel()
	for i := 0; i < count; i++ {
		duc.Add() <- uuid.New()
	}
	assert.Equal(t, count, <-duc.Count())
	waiter := sync.WaitGroup{}
	waiter.Add(count)
	for i := 0; i < 50; i++ {
		go func() {
			for {
				<-duc.Get()
				waiter.Done()
			}
		}()
	}
	waiter.Wait()
	assert.Equal(t, 0, <-duc.Count())
	count = int(math.Max(400, float64(rand.Intn(1000))))
	waiter.Add(count)
	for i := 0; i < count; i++ {
		duc.Add() <- uuid.New()
	}
	waiter.Wait()
	assert.Equal(t, 0, <-duc.Count())
}

func TestDynamicMsgChannel(t *testing.T) {
	count := int(math.Max(400, float64(rand.Intn(1000))))
	dmc := NewDynamicMsgChannel()
	for i := 0; i < count; i++ {
		dmc.Add() <- &pubsub.ReceivedMessage{}
	}
	assert.Equal(t, count, <-dmc.Count())
	waiter := sync.WaitGroup{}
	waiter.Add(count)
	for i := 0; i < 50; i++ {
		go func() {
			for {
				<-dmc.Get()
				waiter.Done()
			}
		}()
	}
	waiter.Wait()
	assert.Equal(t, 0, <-dmc.Count())
	count = int(math.Max(400, float64(rand.Intn(1000))))
	waiter.Add(count)
	for i := 0; i < count; i++ {
		dmc.Add() <- &pubsub.ReceivedMessage{}
	}
	waiter.Wait()
	assert.Equal(t, 0, <-dmc.Count())
}
