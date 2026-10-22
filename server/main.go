package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"unicode/utf8"

	pb "ChitChat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedChitChatServer
	mu      sync.Mutex
	clock   int64
	clients map[string]chan *pb.ServerMessage
}

// tick applies the Lamport rule. Caller must hold s.mu.
func (s *server) tick(received int64) int64 {
	s.clock = max(s.clock, received) + 1
	return s.clock
}

// broadcast puts a message on every client's channel. Caller must hold s.mu.
func (s *server) broadcast(sender, content string, l int64) {
	msg := &pb.ServerMessage{SenderId: sender, Content: content, LamportTime: l}
	for id, ch := range s.clients {
		ch <- msg
		log.Printf("event=DELIVER target=%s lamport=%d content=%q", id, l, content)
	}
}

// Chat runs in its own goroutine for each connected client.
func (s *server) Chat(stream pb.ChitChat_ChatServer) error {
	first, err := stream.Recv() // first message is the JOIN
	if err != nil {
		return err
	}
	id := first.ClientId

	out := make(chan *pb.ServerMessage, 100)
	s.mu.Lock()
	if _, taken := s.clients[id]; taken || id == "" {
		s.mu.Unlock()
		return status.Error(codes.AlreadyExists, "name empty or already in use")
	}
	s.clients[id] = out
	l := s.tick(first.LamportTime)
	log.Printf("event=CLIENT_JOINED client=%s lamport=%d", id, l)
	s.broadcast("server", fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", id, l), l)
	s.mu.Unlock()

	go func() { // forwards broadcasts to this client's stream
		for msg := range out {
			stream.Send(msg)
		}
	}()

	lastTime := first.LamportTime
	for {
		msg, err := stream.Recv()
		if err != nil {
			break
		}
		lastTime = msg.LamportTime
		if msg.Type == pb.MessageType_LEAVE {
			break
		}
		if utf8.ValidString(msg.Content) && utf8.RuneCountInString(msg.Content) <= 128 {
			s.mu.Lock()
			l := s.tick(msg.LamportTime)
			log.Printf("event=PUBLISH client=%s lamport=%d content=%q", id, l, msg.Content)
			s.broadcast(id, msg.Content, l)
			s.mu.Unlock()
		}
	}

	s.mu.Lock()
	delete(s.clients, id)
	close(out)
	l = s.tick(lastTime)
	log.Printf("event=CLIENT_LEFT client=%s lamport=%d", id, l)
	s.broadcast("server", fmt.Sprintf("Participant %s left Chit Chat at logical time %d", id, l), l)
	s.mu.Unlock()
	return nil
}

func main() {
	f, _ := os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	log.SetOutput(io.MultiWriter(os.Stdout, f))
	log.SetPrefix("[Server] ")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		log.Println("event=SERVER_SHUTDOWN")
		os.Exit(0)
	}()

	grpcServer := grpc.NewServer()
	pb.RegisterChitChatServer(grpcServer, &server{clients: map[string]chan *pb.ServerMessage{}})
	log.Println("event=SERVER_STARTUP addr=" + lis.Addr().String())
	grpcServer.Serve(lis)
}
