package mempubsub

import (
	"fmt"
	"strings"
	"sync"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"github.com/lwahlmeier/lcwlog"
)

var logger = lcwlog.GetLoggerWithPrefix("MemPubSub")

type MemBase struct {
	projects    map[string]*MemProject
	projectLock sync.Mutex
}

func NewMemBase() *MemBase {
	logger.Info("Creating MemoryBase")
	return &MemBase{
		projects: make(map[string]*MemProject),
	}
}

func (mb *MemBase) ParseProjectName(project string) (string, error) {
	items := strings.Split(project, "/")
	if len(items) >= 2 && items[0] == "projects" {
		return items[1], nil
	}
	logger.Warn("Got bad Project name:{}", project)
	return "", fmt.Errorf("bad project name: %s", project)
}
func (mb *MemBase) ParseProjectAndTopicName(topicName string) (string, string, error) {
	pjName, err := mb.ParseProjectName(topicName)
	if err != nil {
		return "", "", err
	}
	items := strings.Split(topicName, "/")
	if len(items) >= 4 && items[0] == "projects" && items[2] == "topics" {
		return pjName, items[3], nil
	}
	logger.Warn("Got bad Topic name:{}", topicName)
	return "", "", fmt.Errorf("bad topic name: %s", topicName)
}
func (mb *MemBase) ParseProjectAndSubscriptionName(subName string) (string, string, error) {
	pjName, err := mb.ParseProjectName(subName)
	if err != nil {
		return "", "", err
	}
	items := strings.Split(subName, "/")
	if len(items) >= 2 && items[0] == "projects" && items[2] == "subscriptions" {
		return pjName, items[3], nil
	}
	logger.Warn("Got bad Subject name:{}", subName)
	return "", "", fmt.Errorf("bad subscription name: %s", subName)
}

func (mb *MemBase) GetProject(pjName string) (base.BaseProject, error) {
	mb.projectLock.Lock()
	defer mb.projectLock.Unlock()
	if pjt, ok := mb.projects[pjName]; ok {
		return pjt, nil
	}
	pjt := NewMemProject(pjName, mb)
	mb.projects[pjName] = pjt
	return pjt, nil
}

func (mb *MemBase) DeleteProject(pjName string) {
	mb.projectLock.Lock()
	defer mb.projectLock.Unlock()
	delete(mb.projects, pjName)
}
