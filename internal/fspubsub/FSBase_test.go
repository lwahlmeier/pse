package fspubsub

import (
	"log"
	"math/rand"
	"os"
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

func TestFSBaseProjectCreate(t *testing.T) {
	count := 100
	pjtNames := make([]string, count)
	for i := 0; i < count; i++ {
		pjtNames[i] = makeString(20)
	}

	dir, err := os.MkdirTemp("", "go-test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	os.MkdirAll(dir, os.ModePerm)
	fsb, err := NewFSBase(dir)
	if err != nil {
		log.Fatal(err)
	}
	for _, pjt := range pjtNames {
		fsb.GetProject(pjt)
	}

	fsb, err = NewFSBase(dir)
	if err != nil {
		log.Fatal(err)
	}
	for _, pjt := range pjtNames {
		_, ok := fsb.projects[pjt]
		assert.True(t, ok)
	}

	for _, pjt := range pjtNames {
		fsb.DeleteProject(pjt)
	}

	for _, pjt := range pjtNames {
		_, ok := fsb.projects[pjt]
		assert.False(t, ok)
	}
}

func TestParseProject(t *testing.T) {
	dir, err := os.MkdirTemp("", "go-test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(dir, os.ModePerm)
	fsb, err := NewFSBase(dir)
	assert.Nil(t, err)
	pjt, err := fsb.ParseProjectName("projects/somename")
	assert.Nil(t, err)
	assert.Equal(t, "somename", pjt)
	pjt, err = fsb.ParseProjectName("badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	pjt, err = fsb.ParseProjectName("more/badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
}

func TestParseTopic(t *testing.T) {
	dir, err := os.MkdirTemp("", "go-test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(dir, os.ModePerm)
	fsb, err := NewFSBase(dir)
	assert.Nil(t, err)

	pjt, topic, err := fsb.ParseProjectAndTopicName("projects/somename/topics/sometopic")
	assert.Nil(t, err)
	assert.Equal(t, "somename", pjt)
	assert.Equal(t, "sometopic", topic)
	pjt, topic, err = fsb.ParseProjectAndTopicName("badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	assert.Equal(t, "", topic)
	pjt, topic, err = fsb.ParseProjectAndTopicName("projects/more/badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	assert.Equal(t, "", topic)
}

func TestParseSubscriptions(t *testing.T) {
	dir, err := os.MkdirTemp("", "go-test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)
	os.MkdirAll(dir, os.ModePerm)
	fsb, err := NewFSBase(dir)
	assert.Nil(t, err)

	pjt, sub, err := fsb.ParseProjectAndSubscriptionName("projects/somename/subscriptions/somesub")
	assert.Nil(t, err)
	assert.Equal(t, "somename", pjt)
	assert.Equal(t, "somesub", sub)
	pjt, sub, err = fsb.ParseProjectAndSubscriptionName("badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	assert.Equal(t, "", sub)
	pjt, sub, err = fsb.ParseProjectAndSubscriptionName("projects/more/badstring")
	assert.Error(t, err)
	assert.Equal(t, "", pjt)
	assert.Equal(t, "", sub)
}
