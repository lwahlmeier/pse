package fspubsub

import (
	"errors"
	"os"
	"path"
	"sync"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FSProject struct {
	fsBase      *FSBase
	name        string
	topics      map[string]*FSTopic
	projectPath string
	lock        sync.Mutex
}

func CreateFSProject(name string, fsb *FSBase) (*FSProject, error) {
	projectPath := path.Join(fsb.basePath, name)
	_, err := os.Stat(projectPath)
	if err == nil {
		return nil, status.Error(codes.AlreadyExists, "Project Already Exists!")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	err = os.MkdirAll(projectPath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	logger.Info("Creating Project:{}", name)
	return &FSProject{
		fsBase:      fsb,
		name:        name,
		projectPath: projectPath,
		topics:      make(map[string]*FSTopic),
	}, nil

}

func LoadFSProject(name string, fsb *FSBase) (*FSProject, error) {
	logger.Info("Loading project:{}", name)
	projectPath := path.Join(fsb.basePath, name)
	stat, err := os.Stat(projectPath)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, status.Error(codes.Internal, "Can not create project")
	}
	pj := &FSProject{
		fsBase:      fsb,
		projectPath: projectPath,
		name:        name,
		topics:      make(map[string]*FSTopic),
	}
	err = pj.loadTopics()
	if err != nil {
		return nil, err
	}
	return pj, nil
}

func (fsp *FSProject) loadTopics() error {
	fsil, err := os.ReadDir(fsp.projectPath)
	if err != nil {
		return err
	}
	fsTopics := make([]*FSTopic, 0)
	for _, fsi := range fsil {
		if !fsi.IsDir() {
			continue
		}
		fsTopic, err := LoadFSTopic(fsi.Name(), fsp)
		if err != nil {
			continue
		}
		fsTopics = append(fsTopics, fsTopic)
	}
	fsp.lock.Lock()
	defer fsp.lock.Unlock()
	for _, fsTopic := range fsTopics {
		fsp.topics[fsTopic.name] = fsTopic
	}
	return nil
}

func (fsp *FSProject) GetBase() base.BaseBackend {
	return fsp.fsBase
}

func (fsp *FSProject) GetName() string {
	return fsp.name
}

func (pjt *FSProject) CreateTopic(topic *pubsub.Topic) error {
	_, tn, err := pjt.fsBase.ParseProjectAndTopicName(topic.Name)
	if err != nil {
		return err
	}
	pjt.lock.Lock()
	defer pjt.lock.Unlock()
	if _, ok := pjt.topics[tn]; ok {
		return status.Error(codes.AlreadyExists, "Topic Already Exists")
	}
	fsTopic, err := CreateFSTopic(pjt, topic)
	if err != nil {
		return err
	}
	pjt.topics[tn] = fsTopic
	return nil
}

func (pjt *FSProject) GetTopic(topicName string) base.BaseTopic {
	pjt.lock.Lock()
	defer pjt.lock.Unlock()
	if topic, ok := pjt.topics[topicName]; ok {
		return topic
	}
	return nil
}

func (pjt *FSProject) DeleteTopic(topicName string) {
	pjt.lock.Lock()
	defer pjt.lock.Unlock()
	delete(pjt.topics, topicName)
}

func (pjt *FSProject) GetAllTopics() map[string]base.BaseTopic {
	pjt.lock.Lock()
	defer pjt.lock.Unlock()
	topics := make(map[string]base.BaseTopic)
	for name, topic := range pjt.topics {
		topics[name] = topic
	}
	return topics
}

func (pjt *FSProject) GetAllSubscriptions() map[string]base.BaseSubscription {
	pjt.lock.Lock()
	defer pjt.lock.Unlock()
	subs := make(map[string]base.BaseSubscription)
	for _, fsTopic := range pjt.topics {
		for _, fsSub := range fsTopic.GetAllSubs() {
			subs[fsSub.GetName()] = fsSub
		}
	}
	return subs
}
