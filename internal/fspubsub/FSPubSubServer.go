package fspubsub

import (
	"context"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/lwahlmeier/lcwlog"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var logger = lcwlog.GetLoggerWithPrefix("fspubsub")

type FSPubSubServer struct {
	basePath string
	projects map[string]*FSProject
	mapLock  sync.Mutex
}

func NewFSPubSubServer(basePath string) *FSPubSubServer {
	ps := &FSPubSubServer{
		basePath: basePath,
		projects: make(map[string]*FSProject),
	}
	ps.init()
	return ps
}

func (ps *FSPubSubServer) init() error {
	err := os.MkdirAll(ps.basePath, os.ModePerm)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(ps.basePath)
	if err != nil {
		return err
	}
	ps.mapLock.Lock()
	defer ps.mapLock.Unlock()
	for _, fsi := range entries {
		if fsi.IsDir() {
			fsProject, err := LoadProject(fsi.Name(), ps.basePath)
			if err != nil {
				logger.Warn("Problems loading Project:{}", fsi.Name())
				continue
			}
			logger.Info("Loaded Project:{}", fsi.Name())
			ps.projects[fsi.Name()] = fsProject
		}
	}
	return nil
}

func (ps *FSPubSubServer) CreateTopic(ctx context.Context, topic *pubsub.Topic) (*pubsub.Topic, error) {
	pjName, tn, err := GetProjectAndTopic(topic.Name)
	if err != nil {
		return nil, err
	}
	logger.Info("Project:{}, Got CreateTopic: {}", pjName, tn)
	ps.mapLock.Lock()
	defer ps.mapLock.Unlock()
	fsProject, err := CreateProject(pjName, ps.basePath)
	if err != nil {
		return nil, err
	}
	ps.projects[pjName] = fsProject
	err = fsProject.AddTopic(topic)
	if err != nil {
		return nil, err
	}
	return topic, nil
}

func (ps *FSPubSubServer) GetProject(pjName string) *FSProject {
	ps.mapLock.Lock()
	defer ps.mapLock.Unlock()
	return ps.projects[pjName]
}

func (ps *FSPubSubServer) UpdateTopic(ctx context.Context, utr *pubsub.UpdateTopicRequest) (*pubsub.Topic, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateTopic not implemented")
}

func (ps *FSPubSubServer) Publish(ctx context.Context, pr *pubsub.PublishRequest) (*pubsub.PublishResponse, error) {
	logger.Info("Publishing Message")
	pjName, topicName, err := GetProjectAndTopic(pr.Topic)
	if err != nil {
		return nil, err
	}
	logger.Info("Project:{}, Got Publish to:{}", pjName, topicName)
	fsProject := ps.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	fsTopic := fsProject.GetTopic(topicName)
	if fsTopic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}
	mids := make([]string, 0)
	for _, msg := range pr.Messages {
		msg.MessageId = uuid.New().String()
		err = fsTopic.Publish(msg)
		if err != nil {
			logger.Warn("Project:{}, Topic:{} error publishing:{}", pjName, topicName, err.Error())
		}
		mids = append(mids, msg.MessageId)
	}
	return &pubsub.PublishResponse{MessageIds: mids}, nil
}

func (ps *FSPubSubServer) GetTopic(ctx context.Context, rTopic *pubsub.GetTopicRequest) (*pubsub.Topic, error) {
	pjName, topicName, err := GetProjectAndTopic(rTopic.Topic)
	if err != nil {
		return nil, err
	}
	logger.Info("Project:{}, Got GetTopic: {}", pjName, topicName)
	fsProject := ps.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	fsTopic := fsProject.GetTopic(topicName)
	if fsTopic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}
	return fsTopic.GetPubSubTopic(), nil
}

func (ps *FSPubSubServer) ListTopics(ctx context.Context, ltr *pubsub.ListTopicsRequest) (*pubsub.ListTopicsResponse, error) {
	pjName, err := GetProjectName(ltr.Project)
	if err != nil {
		return nil, err
	}
	fsProject := ps.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	return &pubsub.ListTopicsResponse{Topics: fsProject.GetAllTopics()}, nil
}
func (ps *FSPubSubServer) ListTopicSubscriptions(ctx context.Context, ltsr *pubsub.ListTopicSubscriptionsRequest) (*pubsub.ListTopicSubscriptionsResponse, error) {
	pjName, topicName, err := GetProjectAndTopic(ltsr.Topic)
	if err != nil {
		return nil, err
	}
	fsProject := ps.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	fsTopic := fsProject.GetTopic(topicName)
	if fsTopic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}

	subs := fsTopic.GetAllSubscriptions()
	subPaths := make([]string, 0)
	for _, sub := range subs {
		subPaths = append(subPaths, sub.GetPubSubSubscription().Name)
	}
	return &pubsub.ListTopicSubscriptionsResponse{Subscriptions: subPaths}, nil
}

func (pc *FSPubSubServer) ListTopicSnapshots(context.Context, *pubsub.ListTopicSnapshotsRequest) (*pubsub.ListTopicSnapshotsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListTopicSnapshots not implemented")
}
func (ps *FSPubSubServer) DeleteTopic(ctx context.Context, dtr *pubsub.DeleteTopicRequest) (*emptypb.Empty, error) {
	pjName, topicName, err := GetProjectAndTopic(dtr.Topic)
	if err != nil {
		return nil, err
	}
	fsProject := ps.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	fsTopic := fsProject.GetTopic(topicName)
	if fsTopic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}
	fsTopic.Delete()
	return nil, nil
}
func (pc *FSPubSubServer) DetachSubscription(context.Context, *pubsub.DetachSubscriptionRequest) (*pubsub.DetachSubscriptionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DetachSubscription not implemented")
}

//Sub functions

func (ps *FSPubSubServer) CreateSubscription(ctx context.Context, sub *pubsub.Subscription) (*pubsub.Subscription, error) {
	pjName, topicName, err := GetProjectAndTopic(sub.Topic)
	if err != nil {
		return nil, err
	}
	subName, err := GetSubscriptionName(sub.Name)
	if err != nil {
		return nil, err
	}
	fsProject := ps.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	fsTopic := fsProject.GetTopic(topicName)
	if fsTopic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}
	err = fsTopic.AddSub(sub)
	if err != nil {
		logger.Info("Project:{},Topic:{} error creating Sub:{}", pjName, topicName, err.Error())
		return nil, err
	}
	logger.Info("Project:{},Topic:{},Sub:{}", pjName, topicName, subName)
	return sub, nil
}

func (fspss *FSPubSubServer) GetSubscription(ctx context.Context, sr *pubsub.GetSubscriptionRequest) (*pubsub.Subscription, error) {
	pjName, err := GetProjectName(sr.Subscription)
	if err != nil {
		return nil, err
	}
	subName, err := GetSubscriptionName(sr.Subscription)
	if err != nil {
		return nil, err
	}
	fsProject := fspss.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	sub := fsProject.GetSubscription(subName)
	if sub == nil {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	return sub.GetPubSubSubscription(), nil
}

func (ss *FSPubSubServer) UpdateSubscription(context.Context, *pubsub.UpdateSubscriptionRequest) (*pubsub.Subscription, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateSubscription not implemented")
}
func (fspss *FSPubSubServer) ListSubscriptions(ctx context.Context, lsr *pubsub.ListSubscriptionsRequest) (*pubsub.ListSubscriptionsResponse, error) {
	pjName, err := GetProjectName(lsr.Project)
	if err != nil {
		return nil, err
	}
	fsProject := fspss.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	subs := fsProject.GetAllSubscription()
	return &pubsub.ListSubscriptionsResponse{Subscriptions: subs}, nil
}
func (fspss *FSPubSubServer) DeleteSubscription(ctx context.Context, dsr *pubsub.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	pjName, err := GetProjectName(dsr.Subscription)
	if err != nil {
		return nil, err
	}
	subName, err := GetSubscriptionName(dsr.Subscription)
	if err != nil {
		return nil, err
	}
	fsProject := fspss.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	sub := fsProject.GetSubscription(subName)
	if sub == nil {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	sub.Delete()
	return nil, nil
}
func (fspss *FSPubSubServer) ModifyAckDeadline(ctx context.Context, madr *pubsub.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	logger.Info("ModifyAckDeadline")
	pjName, err := GetProjectName(madr.Subscription)
	if err != nil {
		return nil, err
	}
	subName, err := GetSubscriptionName(madr.Subscription)
	if err != nil {
		return nil, err
	}
	fsProject := fspss.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	sub := fsProject.GetSubscription(subName)
	if sub == nil {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	sub.UpdateAcks(madr.AckIds, madr.AckDeadlineSeconds)
	return &emptypb.Empty{}, nil
}
func (fspss *FSPubSubServer) Acknowledge(ctx context.Context, ar *pubsub.AcknowledgeRequest) (*emptypb.Empty, error) {
	logger.Info("Acknowledge")
	pjName, err := GetProjectName(ar.Subscription)
	if err != nil {
		return nil, err
	}
	subName, err := GetSubscriptionName(ar.Subscription)
	if err != nil {
		return nil, err
	}
	fsProject := fspss.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	sub := fsProject.GetSubscription(subName)
	if sub == nil {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	sub.AckMessages(ar.AckIds)
	return &emptypb.Empty{}, nil
}
func (fspss *FSPubSubServer) Pull(ctx context.Context, pr *pubsub.PullRequest) (*pubsub.PullResponse, error) {
	logger.Info("Pull")
	pjName, err := GetProjectName(pr.Subscription)
	if err != nil {
		return nil, err
	}
	subName, err := GetSubscriptionName(pr.Subscription)
	if err != nil {
		return nil, err
	}
	fsProject := fspss.GetProject(pjName)
	if fsProject == nil {
		return nil, status.Error(codes.NotFound, "Invalid project")
	}
	sub := fsProject.GetSubscription(subName)
	if sub == nil {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	msgs := sub.GetMessages(pr.MaxMessages)
	return &pubsub.PullResponse{ReceivedMessages: msgs}, nil
}

func (fspss *FSPubSubServer) StreamingPull(pullServer pubsub.Subscriber_StreamingPullServer) error {
	pullRequest, err := pullServer.Recv()
	if err != nil {
		return err
	}
	pjName, err := GetProjectName(pullRequest.Subscription)
	if err != nil {
		return err
	}
	subName, err := GetSubscriptionName(pullRequest.Subscription)
	if err != nil {
		return err
	}
	fsProject := fspss.GetProject(pjName)
	if fsProject == nil {
		return status.Error(codes.NotFound, "Invalid project")
	}
	sub := fsProject.GetSubscription(subName)
	if sub == nil {
		return status.Error(codes.NotFound, "Subscription not found")
	}

	// for {

	// }
	return status.Errorf(codes.Unimplemented, "method StreamingPull not implemented")
}
func (ss *FSPubSubServer) ModifyPushConfig(context.Context, *pubsub.ModifyPushConfigRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ModifyPushConfig not implemented")
}
func (ss *FSPubSubServer) GetSnapshot(context.Context, *pubsub.GetSnapshotRequest) (*pubsub.Snapshot, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSnapshot not implemented")
}
func (ss *FSPubSubServer) ListSnapshots(context.Context, *pubsub.ListSnapshotsRequest) (*pubsub.ListSnapshotsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListSnapshots not implemented")
}
func (ss *FSPubSubServer) CreateSnapshot(context.Context, *pubsub.CreateSnapshotRequest) (*pubsub.Snapshot, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateSnapshot not implemented")
}
func (ss *FSPubSubServer) UpdateSnapshot(context.Context, *pubsub.UpdateSnapshotRequest) (*pubsub.Snapshot, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateSnapshot not implemented")
}
func (ss *FSPubSubServer) DeleteSnapshot(context.Context, *pubsub.DeleteSnapshotRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteSnapshot not implemented")
}
func (ss *FSPubSubServer) Seek(context.Context, *pubsub.SeekRequest) (*pubsub.SeekResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Seek not implemented")
}
