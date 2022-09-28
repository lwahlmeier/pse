package fspubsub

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"github.com/lwahlmeier/lcwlog"
)

var logger = lcwlog.GetLoggerWithPrefix("FSPubSub")

type FSBase struct {
	basePath string
	projects map[string]*FSProject
	mapLock  sync.Mutex
}

func StartFSBase(basePath string) (*FSBase, error) {
	logger.Info("Starting FSBase at path:{}", basePath)
	err := os.MkdirAll(basePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}
	fsb := &FSBase{
		basePath: basePath,
		projects: make(map[string]*FSProject),
	}
	for _, fsi := range entries {
		if fsi.IsDir() {
			fsProject, err := LoadFSProject(fsi.Name(), fsb)
			if err != nil {
				return nil, err
			}
			fsb.projects[fsi.Name()] = fsProject
		}
	}

	return fsb, nil
}

func (fsb *FSBase) ParseProjectName(project string) (string, error) {
	items := strings.Split(project, "/")
	if len(items) >= 2 && items[0] == "projects" {
		return items[1], nil
	}
	logger.Warn("Got bad project name:{}", project)
	return "", fmt.Errorf("bad project name: %s", project)
}
func (fsb *FSBase) ParseProjectAndTopicName(topicName string) (string, string, error) {
	pjName, err := fsb.ParseProjectName(topicName)
	if err != nil {
		return "", "", err
	}
	items := strings.Split(topicName, "/")
	if len(items) >= 4 && items[0] == "projects" && items[2] == "topics" {
		return pjName, items[3], nil
	}
	logger.Warn("Got bad topic name:{}", topicName)
	return "", "", fmt.Errorf("bad topic name: %s", topicName)
}
func (fsb *FSBase) ParseProjectAndSubscriptionName(subName string) (string, string, error) {
	pjName, err := fsb.ParseProjectName(subName)
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

func (fsb *FSBase) GetProject(pjName string) (base.BaseProject, error) {
	fsb.mapLock.Lock()
	defer fsb.mapLock.Unlock()
	if fsProject, ok := fsb.projects[pjName]; ok {
		return fsProject, nil
	}
	pjt, err := CreateFSProject(pjName, fsb)
	if err != nil {
		return nil, err
	}
	fsb.projects[pjName] = pjt
	return pjt, nil
}
func (fsb *FSBase) DeleteProject(pjName string) {
	fsb.mapLock.Lock()
	defer fsb.mapLock.Unlock()
	if pjt, ok := fsb.projects[pjName]; ok {
		delete(fsb.projects, pjName)
		os.RemoveAll(pjt.projectPath)
		logger.Info("Deleted Project:{}", pjName)
	}
}
