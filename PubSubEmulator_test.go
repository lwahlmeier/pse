package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/fspubsub"
	"githb.com/lwahlmeier/go-pubsub-emulator/internal/mempubsub"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/pubsub/v1"
)

var BASES = []string{"mem", "fs"}

// var BASES = []string{"fs"}
var letterRunes = []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func CreateBase(base string) *PubSubEmulator {
	if base == "mem" {
		return &PubSubEmulator{baseBackend: mempubsub.NewMemBase()}
	}
	dir, _ := os.MkdirTemp("", "go-test")
	os.MkdirAll(dir, os.ModePerm)
	fsb, _ := fspubsub.NewFSBase(dir)
	return &PubSubEmulator{baseBackend: fsb}
}

func makeString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestBasicTopic(t *testing.T) {

	for _, bs := range BASES {
		pse := CreateBase(bs)
		projectName := makeString(30)
		count := 10
		topicNames := make([]string, count)

		topicPubSubs := make([]*pubsub.Topic, count)

		for i := 0; i < count; i++ {
			topicNames[i] = makeString(30)
			topicPubSubs[i] = &pubsub.Topic{Name: fmt.Sprintf("projects/%s/topics/%s", projectName, topicNames[i])}
		}
		for i := 0; i < count; i++ {
			rpst, err := pse.CreateTopic(context.TODO(), topicPubSubs[i])
			assert.NoError(t, err)
			assert.NotNil(t, rpst)
			assert.Equal(t, topicPubSubs[i], rpst)
		}
		for i := 0; i < count; i++ {
			rpst, err := pse.GetTopic(context.TODO(), &pubsub.GetTopicRequest{Topic: topicPubSubs[i].Name})
			assert.NoError(t, err)
			assert.NotNil(t, rpst)
			assert.Equal(t, topicPubSubs[i], rpst)
		}
		tList, err := pse.ListTopics(context.TODO(), &pubsub.ListTopicsRequest{Project: fmt.Sprintf("projects/%s", projectName)})
		assert.NoError(t, err)
		assert.NotNil(t, tList)
		assert.Equal(t, count, len(tList.Topics))
		subs := make(map[string][]string)
		subsPubSub := make(map[string][]*pubsub.Subscription)
		for _, tn := range topicNames {
			subs[tn] = make([]string, count)
			subsPubSub[tn] = make([]*pubsub.Subscription, count)
			for i := 0; i < count; i++ {
				subs[tn][i] = makeString(30)
				subsPubSub[tn][i] = &pubsub.Subscription{
					Name:  fmt.Sprintf("projects/%s/subscriptions/%s", projectName, subs[tn][i]),
					Topic: fmt.Sprintf("projects/%s/topics/%s", projectName, tn),
				}
			}
		}
		for _, subs := range subsPubSub {
			for _, sub := range subs {
				rs, err := pse.CreateSubscription(context.TODO(), sub)
				assert.NoError(t, err)
				assert.NotNil(t, rs)
				assert.Equal(t, sub, rs)
			}
		}
		for _, topic := range topicPubSubs {
			pse.Publish(context.TODO(), &pubsub.PublishRequest{
				Topic:    topic.Name,
				Messages: []*pubsub.PubsubMessage{{Data: []byte("TEST")}},
			})
		}
		for _, subs := range subsPubSub {
			for _, sub := range subs {
				pr, err := pse.Pull(context.TODO(), &pubsub.PullRequest{Subscription: sub.Name, MaxMessages: 1})
				assert.NoError(t, err)
				assert.NotNil(t, pr)
				assert.Equal(t, "TEST", string(pr.ReceivedMessages[0].Message.Data))
				_, err = pse.Acknowledge(context.TODO(), &pubsub.AcknowledgeRequest{Subscription: sub.Name, AckIds: []string{pr.ReceivedMessages[0].AckId}})
				assert.NoError(t, err)
			}
		}
	}
}
