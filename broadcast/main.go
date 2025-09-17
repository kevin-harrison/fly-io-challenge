package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	mu       sync.RWMutex
	messages map[string][]int
	length   int
}

func (s *State) AddMessage(server string, msg int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[server] = append(s.messages[server], msg)
	s.length += 1
}

func (s *State) Sync(server string, syncIdx int, syncSuffix []int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	currMsgCount := len(s.messages[server])
	if syncIdx < 0 || syncIdx > currMsgCount {
		return currMsgCount, fmt.Errorf("Invalid suffix sync: logLen=%v, syncIdx=%v", currMsgCount, syncIdx)
	}
	newElements := syncSuffix[currMsgCount-syncIdx:]
	s.messages[server] = append(s.messages[server], newElements...)
	s.length += len(newElements)
	return len(s.messages[server]), nil
}

func (s *State) GetMessages() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	messages := make([]int, 0, s.length)
	for _, serverMessages := range s.messages {
		messages = append(messages, serverMessages...)
	}
	return messages
}

func (s *State) GetSuffix(server string, from int) ([]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	serverMessages := s.messages[server]
	if from < 0 || from > len(serverMessages) {
		return nil, fmt.Errorf("WARN: Tried to get non-sensical suffix: %v..%v for server %v", from, len(serverMessages), server)
	} else if from == len(serverMessages) {
		return nil, nil
	}
	suffix := make([]int, len(serverMessages)-from)
	copy(suffix, serverMessages[from:])
	return suffix, nil
}

type Ack struct {
	From   string
	AckIdx int
}

func runPeriodicSync(
	ctx context.Context,
	node *maelstrom.Node,
	state *State,
	ackCh <-chan Ack,
) error {
	acks := map[string]int{}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ack := <-ackCh:
			if ack.AckIdx > acks[ack.From] {
				acks[ack.From] = ack.AckIdx
			}
		case <-ticker.C:
			for _, id := range node.NodeIDs() {
				if id == node.ID() {
					continue
				}
				syncIdx := acks[id]
				syncSuffix, err := state.GetSuffix(node.ID(), syncIdx)
				if err != nil {
					log.Fatal(err)
				}
				if syncSuffix != nil {
					syncMsg := SyncMsg{
						Type:       "sync",
						SyncSuffix: syncSuffix,
						SyncIdx:    syncIdx,
					}
					if err := node.Send(id, syncMsg); err != nil {
						log.Printf("WARN: Couldn't send sync message: %v", err)
					}
				}
			}
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	state := State{
		messages: map[string][]int{},
	}
	ackCh := make(chan Ack, 1024)

	n.Handle("broadcast", handleBroadcast(n, &state))
	n.Handle("read", handleRead(n, &state))
	n.Handle("topology", handleTopology(n))
	n.Handle("sync", handleSync(n, &state))
	n.Handle("sync_ok", handleSyncOk(ackCh))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runPeriodicSync(ctx, n, &state, ackCh)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// ---------------------
// Broadcast Handler
// ---------------------

type BroadcastMsg struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type BroadcastAck struct {
	Type string `json:"type"`
}

func handleBroadcast(node *maelstrom.Node, state *State) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body BroadcastMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.AddMessage(node.ID(), body.Message)
		ack := BroadcastAck{
			Type: "broadcast_ok",
		}
		return node.Reply(msg, ack)
	}
}

// ---------------------
// Sync Handlers
// ---------------------

type SyncMsg struct {
	Type       string `json:"type"`
	SyncSuffix []int  `json:"sync_suffix"`
	SyncIdx    int    `json:"sync_idx"`
}

type SyncAck struct {
	Type   string `json:"type"`
	AckIdx int    `json:"ack_idx"`
}

func handleSync(node *maelstrom.Node, state *State) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body SyncMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		newLen, err := state.Sync(msg.Src, body.SyncIdx, body.SyncSuffix)
		if err != nil {
			log.Fatal(err)
		}
		ack := SyncAck{
			Type:   "sync_ok",
			AckIdx: newLen,
		}
		return node.Reply(msg, ack)
	}
}

func handleSyncOk(ackCh chan Ack) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body SyncAck
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		select {
		case ackCh <- Ack{From: msg.Src, AckIdx: body.AckIdx}:
		default:
			log.Printf("WARN: dropping ack from %s (channel full)", msg.Src)
		}
		return nil
	}
}

// ---------------------
// Read Handler
// ---------------------

type ReadMsg struct {
	Type string `json:"type"`
}

type ReadAck struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func handleRead(node *maelstrom.Node, state *State) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body ReadMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := state.GetMessages()
		ack := ReadAck{
			Type:     "read_ok",
			Messages: messages,
		}
		return node.Reply(msg, ack)

	}
}

// ---------------------
// Topology Handler
// ---------------------

type TopologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type TopologyAck struct {
	Type string `json:"type"`
}

func handleTopology(node *maelstrom.Node) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body TopologyMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ack := TopologyAck{
			Type: "topology_ok",
		}
		return node.Reply(msg, ack)
	}
}
