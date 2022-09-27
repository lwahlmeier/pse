package main

import (
	"log"
	"net"

	pubsub "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc"
)

//TODO: add arg parsing
func main() {
	lis, err := net.Listen("tcp", "localhost:8681")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	mypub, err := NewFileSystemPubSubEmulator("/tmp/pubsub2")
	// mypub := NewMemoryPubSubEmulator()
	pubsub.RegisterPublisherServer(grpcServer, mypub)
	pubsub.RegisterSubscriberServer(grpcServer, mypub)
	grpcServer.Serve(lis)
}
