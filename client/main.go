package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"unicode/utf8"

	pb "ChitChat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	mu    sync.Mutex
	clock int64
)

// tick applies the Lamport rule (pass 0 for a send event).
func tick(received int64) int64 {
	mu.Lock()
	defer mu.Unlock()
	clock = max(clock, received) + 1
	return clock
}

func main() {
	name := flag.String("name", "", "your participant name")
	addr := flag.String("addr", "localhost:50051", "server address")
	flag.Parse()

	f, _ := os.OpenFile("client-"+*name+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	logger := log.New(f, "[Client "+*name+"] ", log.LstdFlags)

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	stream, err := pb.NewChitChatClient(conn).Chat(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	send := func(t pb.MessageType, content string) {
		l := tick(0)
		logger.Printf("event=SEND type=%s lamport=%d content=%q", t, l, content)
		stream.Send(&pb.ClientMessage{Type: t, ClientId: *name, Content: content, LamportTime: l})
	}

	done := make(chan struct{})
	go func() { // receive and display broadcasts
		defer close(done)
		for {
			m, err := stream.Recv()
			if err != nil {
				return
			}
			tick(m.LamportTime)
			fmt.Printf("[T=%d] %s: %s\n", m.LamportTime, m.SenderId, m.Content)
			logger.Printf("event=DELIVER lamport=%d sender=%s content=%q", m.LamportTime, m.SenderId, m.Content)
		}
	}()

	send(pb.MessageType_JOIN, "")
	fmt.Println("Type messages and press Enter. Type /leave to quit.")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "/leave" {
			break
		}
		if line == "" || !utf8.ValidString(line) || utf8.RuneCountInString(line) > 128 {
			fmt.Println("Invalid message (must be 1-128 UTF-8 characters).")
			continue
		}
		send(pb.MessageType_PUBLISH, line)
	}
	send(pb.MessageType_LEAVE, "")
	stream.CloseSend()
	<-done
}
