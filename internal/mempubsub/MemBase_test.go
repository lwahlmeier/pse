package mempubsub

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

var letterRunes = []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func makeString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestMemBase(t *testing.T) {
	count := 100
	pjtNames := make([]string, count)
	for i := 0; i < count; i++ {
		pjtNames[i] = makeString(20)
	}
	mb := NewMemBase()
	for _, pName := range pjtNames {
		pjt, err := mb.GetProject(pName)
		assert.NoError(t, err)
		assert.NotNil(t, pjt)
	}
	for _, pName := range pjtNames {
		pjt, err := mb.GetProject(pName)
		assert.NoError(t, err)
		assert.NotNil(t, pjt)
	}
	for _, pName := range pjtNames {
		mb.DeleteProject(pName)
	}
	assert.Equal(t, 0, len(mb.projects))
}

func TestParseProject(t *testing.T) {
	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, err := mb.ParseProjectName("projects/somename")
	assert.Nil(t, err)
	assert.Equal(t, "somename", pjt)
	pjt, err = mb.ParseProjectName("badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	pjt, err = mb.ParseProjectName("more/badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
}

func TestParseTopic(t *testing.T) {
	mb := NewMemBase()
	assert.NotNil(t, mb)
	pjt, topic, err := mb.ParseProjectAndTopicName("projects/somename/topics/sometopic")
	assert.Nil(t, err)
	assert.Equal(t, "somename", pjt)
	assert.Equal(t, "sometopic", topic)
	pjt, topic, err = mb.ParseProjectAndTopicName("badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	assert.Equal(t, "", topic)
	pjt, topic, err = mb.ParseProjectAndTopicName("projects/more/badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	assert.Equal(t, "", topic)
}

func TestParseSubscriptions(t *testing.T) {
	mb := NewMemBase()
	assert.NotNil(t, mb)

	pjt, sub, err := mb.ParseProjectAndSubscriptionName("projects/somename/subscriptions/somesub")
	assert.Nil(t, err)
	assert.Equal(t, "somename", pjt)
	assert.Equal(t, "somesub", sub)
	pjt, sub, err = mb.ParseProjectAndSubscriptionName("badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	assert.Equal(t, "", sub)
	pjt, sub, err = mb.ParseProjectAndSubscriptionName("projects/more/badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	assert.Equal(t, "", sub)
}
