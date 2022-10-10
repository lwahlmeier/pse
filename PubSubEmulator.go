package main

import (
	"context"
	"time"

	"githb.com/lwahlmeier/go-pubsub-emulator/internal/base"
	"github.com/google/uuid"
	"github.com/lwahlmeier/lcwlog"
	"google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var logger = lcwlog.GetLoggerWithPrefix("PubSubEmulator")

type PubSubEmulator struct {
	baseBackend base.BaseBackend
}

func (ps *PubSubEmulator) CreateTopic(ctx context.Context, topic *pubsub.Topic) (*pubsub.Topic, error) {
	pjName, tn, err := ps.baseBackend.ParseProjectAndTopicName(topic.Name)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got CreateTopic for Project:{}, Topic: {}", pjName, tn)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	err = project.CreateTopic(topic)
	if err != nil {
		return nil, err
	}
	return topic, nil
}

func (ps *PubSubEmulator) UpdateTopic(ctx context.Context, utr *pubsub.UpdateTopicRequest) (*pubsub.Topic, error) {
	logger.Warn("Got UpdateTopic, Not implemented")
	return nil, status.Errorf(codes.Unimplemented, "method UpdateTopic not implemented")
}

func (ps *PubSubEmulator) Publish(ctx context.Context, pr *pubsub.PublishRequest) (*pubsub.PublishResponse, error) {
	pjName, topicName, err := ps.baseBackend.ParseProjectAndTopicName(pr.Topic)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got Public for Project:{}, Topic:{}", pjName, topicName)
	fsProject, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	topic := fsProject.GetTopic(topicName)
	if topic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}
	mids := make([]string, 0)
	for _, msg := range pr.Messages {
		msg.PublishTime = timestamppb.Now()
		msg.MessageId = uuid.NewString()
		logger.Info("{}:{} Published mid:{}", pjName, topicName, msg.MessageId)
		err = topic.PublishMessage(msg)
		if err != nil {
			logger.Warn("Project:{}, Topic:{} error publishing:{}", pjName, topicName, err.Error())
		}
		mids = append(mids, msg.MessageId)
	}
	return &pubsub.PublishResponse{MessageIds: mids}, nil
}

func (ps *PubSubEmulator) GetTopic(ctx context.Context, rTopic *pubsub.GetTopicRequest) (*pubsub.Topic, error) {
	pjName, topicName, err := ps.baseBackend.ParseProjectAndTopicName(rTopic.Topic)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got GetTopic for Project:{}, Topic: {}", pjName, topicName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	topic := project.GetTopic(topicName)
	if topic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}
	return topic.GetTopicPubSub(), nil
}

func (ps *PubSubEmulator) ListTopics(ctx context.Context, ltr *pubsub.ListTopicsRequest) (*pubsub.ListTopicsResponse, error) {
	pjName, err := ps.baseBackend.ParseProjectName(ltr.Project)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got ListTopics for Project:{}", pjName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	topics := make([]*pubsub.Topic, 0)
	for _, topic := range project.GetAllTopics() {
		topics = append(topics, topic.GetTopicPubSub())
	}
	return &pubsub.ListTopicsResponse{Topics: topics}, nil
}
func (ps *PubSubEmulator) ListTopicSubscriptions(ctx context.Context, ltsr *pubsub.ListTopicSubscriptionsRequest) (*pubsub.ListTopicSubscriptionsResponse, error) {
	pjName, topicName, err := ps.baseBackend.ParseProjectAndTopicName(ltsr.Topic)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got ListTopicSubscriptions for Project:{}, Topic: {}", pjName, topicName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	topic := project.GetTopic(topicName)
	if topic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}

	subs := topic.GetAllSubs()
	subPaths := make([]string, 0)
	for _, sub := range subs {
		subPaths = append(subPaths, sub.GetSubscriptionPubSub().Name)
	}
	return &pubsub.ListTopicSubscriptionsResponse{Subscriptions: subPaths}, nil
}

func (pc *PubSubEmulator) ListTopicSnapshots(context.Context, *pubsub.ListTopicSnapshotsRequest) (*pubsub.ListTopicSnapshotsResponse, error) {
	logger.Warn("ListTopicSnapshots not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method ListTopicSnapshots not implemented")
}
func (ps *PubSubEmulator) DeleteTopic(ctx context.Context, dtr *pubsub.DeleteTopicRequest) (*emptypb.Empty, error) {
	pjName, topicName, err := ps.baseBackend.ParseProjectAndTopicName(dtr.Topic)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got DeleteTopic for Project:{}, Topic: {}", pjName, topicName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	topic := project.GetTopic(topicName)
	if topic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}
	project.DeleteTopic(topicName)
	return nil, nil
}
func (pc *PubSubEmulator) DetachSubscription(context.Context, *pubsub.DetachSubscriptionRequest) (*pubsub.DetachSubscriptionResponse, error) {
	logger.Warn("DetachSubscription not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method DetachSubscription not implemented")
}

//Sub functions

func (ps *PubSubEmulator) CreateSubscription(ctx context.Context, sub *pubsub.Subscription) (*pubsub.Subscription, error) {
	pjName, topicName, err := ps.baseBackend.ParseProjectAndTopicName(sub.Topic)
	if err != nil {
		return nil, err
	}
	_, subName, err := ps.baseBackend.ParseProjectAndSubscriptionName(sub.Name)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got CreateSubscription for Project:{}, Topic: {}, Sub:{}", pjName, topicName, subName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	subs := project.GetAllSubscriptions()
	//Cant have duplicate sub names in the same project
	for prevSubName, _ := range subs {
		if subName == prevSubName {
			return nil, status.Error(codes.AlreadyExists, "Subject name already exists")
		}
	}
	topic := project.GetTopic(topicName)
	if topic == nil {
		return nil, status.Error(codes.NotFound, "Invalid topic")
	}
	err = topic.CreateSub(sub)
	if err != nil {
		logger.Warn("Project:{},Topic:{} error creating Sub:{}", pjName, topicName, err.Error())
		return nil, err
	}
	return sub, nil
}

func (ps *PubSubEmulator) GetSubscription(ctx context.Context, sr *pubsub.GetSubscriptionRequest) (*pubsub.Subscription, error) {
	pjName, subName, err := ps.baseBackend.ParseProjectAndSubscriptionName(sr.Subscription)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got GetSubscription for Project:{} Sub:{}", pjName, subName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	subs := project.GetAllSubscriptions()
	if sub, ok := subs[subName]; ok {
		return sub.GetSubscriptionPubSub(), nil
	}
	return nil, status.Error(codes.NotFound, "Subscription not found")
}

func (ss *PubSubEmulator) UpdateSubscription(context.Context, *pubsub.UpdateSubscriptionRequest) (*pubsub.Subscription, error) {
	logger.Warn("UpdateSubscription not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method UpdateSubscription not implemented")
}
func (ps *PubSubEmulator) ListSubscriptions(ctx context.Context, lsr *pubsub.ListSubscriptionsRequest) (*pubsub.ListSubscriptionsResponse, error) {
	pjName, err := ps.baseBackend.ParseProjectName(lsr.Project)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got ListSubscriptions for Project:{}", pjName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	subs := project.GetAllSubscriptions()
	pubsubSubs := make([]*pubsub.Subscription, 0)
	for _, sub := range subs {
		pubsubSubs = append(pubsubSubs, sub.GetSubscriptionPubSub())
	}
	return &pubsub.ListSubscriptionsResponse{Subscriptions: pubsubSubs}, nil
}
func (ps *PubSubEmulator) DeleteSubscription(ctx context.Context, dsr *pubsub.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	pjName, subName, err := ps.baseBackend.ParseProjectAndSubscriptionName(dsr.Subscription)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got DeleteSubscription Project:{} Sub:{}", pjName, subName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	subs := project.GetAllSubscriptions()
	if sub, ok := subs[subName]; ok {
		sub.GetTopic().DeleteSub(subName)
	}
	return &emptypb.Empty{}, nil
}
func (ps *PubSubEmulator) ModifyAckDeadline(ctx context.Context, madr *pubsub.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	pjName, subName, err := ps.baseBackend.ParseProjectAndSubscriptionName(madr.Subscription)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got ModifyAckDeadline Project:{} Sub:{}", pjName, subName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}

	subs := project.GetAllSubscriptions()
	sub, ok := subs[subName]
	if !ok {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	sub.UpdateAcks(madr.AckIds, madr.AckDeadlineSeconds)
	return &emptypb.Empty{}, nil

}
func (ps *PubSubEmulator) Acknowledge(ctx context.Context, ar *pubsub.AcknowledgeRequest) (*emptypb.Empty, error) {
	pjName, subName, err := ps.baseBackend.ParseProjectAndSubscriptionName(ar.Subscription)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got Acknowledge Project:{} Sub:{}", pjName, subName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}

	subs := project.GetAllSubscriptions()
	sub, ok := subs[subName]
	if !ok {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	sub.AckMessages(ar.AckIds)
	return &emptypb.Empty{}, nil
}
func (ps *PubSubEmulator) Pull(ctx context.Context, pr *pubsub.PullRequest) (*pubsub.PullResponse, error) {
	pjName, subName, err := ps.baseBackend.ParseProjectAndSubscriptionName(pr.Subscription)
	if err != nil {
		return nil, err
	}
	logger.Debug("Got Pull for Project:{} Sub:{}", pjName, subName)

	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return nil, err
	}
	subs := project.GetAllSubscriptions()
	sub, ok := subs[subName]
	if !ok {
		return nil, status.Error(codes.NotFound, "Subscription not found")
	}
	msgs := sub.GetMessages(pr.MaxMessages, time.Second*10)
	return &pubsub.PullResponse{ReceivedMessages: msgs}, nil
}

func (ps *PubSubEmulator) StreamingPull(pullServer pubsub.Subscriber_StreamingPullServer) error {
	logger.Debug("Got StreamingPull, getting First message")
	pullRequest, err := pullServer.Recv()
	if err != nil {
		return err
	}
	pjName, subName, err := ps.baseBackend.ParseProjectAndSubscriptionName(pullRequest.Subscription)
	if err != nil {
		return err
	}
	logger.Debug("Got StreamingPull for Project:{} and Sub:{}", pjName, subName)
	project, err := ps.baseBackend.GetProject(pjName)
	if err != nil {
		return err
	}
	subs := project.GetAllSubscriptions()
	sub, ok := subs[subName]
	if !ok {
		return status.Error(codes.NotFound, "Subscription not found")
	}
	ss := sub.CreateStreamingSubscription(pullRequest, pullServer)
	ss.Run()
	return err
}
func (ss *PubSubEmulator) ModifyPushConfig(context.Context, *pubsub.ModifyPushConfigRequest) (*emptypb.Empty, error) {
	logger.Warn("Got ModifyPushConfig not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method ModifyPushConfig not implemented")
}
func (ss *PubSubEmulator) GetSnapshot(context.Context, *pubsub.GetSnapshotRequest) (*pubsub.Snapshot, error) {
	logger.Warn("Got GetSnapshot not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method GetSnapshot not implemented")
}
func (ss *PubSubEmulator) ListSnapshots(context.Context, *pubsub.ListSnapshotsRequest) (*pubsub.ListSnapshotsResponse, error) {
	logger.Warn("Got ListSnapshots not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method ListSnapshots not implemented")
}
func (ss *PubSubEmulator) CreateSnapshot(context.Context, *pubsub.CreateSnapshotRequest) (*pubsub.Snapshot, error) {
	logger.Warn("Got CreateSnapshot not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method CreateSnapshot not implemented")
}
func (ss *PubSubEmulator) UpdateSnapshot(context.Context, *pubsub.UpdateSnapshotRequest) (*pubsub.Snapshot, error) {
	logger.Warn("Got UpdateSnapshot not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method UpdateSnapshot not implemented")
}
func (ss *PubSubEmulator) DeleteSnapshot(context.Context, *pubsub.DeleteSnapshotRequest) (*emptypb.Empty, error) {
	logger.Warn("Got DeleteSnapshot not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method DeleteSnapshot not implemented")
}
func (ss *PubSubEmulator) Seek(context.Context, *pubsub.SeekRequest) (*pubsub.SeekResponse, error) {
	logger.Warn("Got Seek not Implemented")
	return nil, status.Errorf(codes.Unimplemented, "method Seek not implemented")
}
