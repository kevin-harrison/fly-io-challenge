package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ServerId = string

type State struct {
	mu     sync.RWMutex
	counts map[ServerId]int
}

func (s *State) Increment(server ServerId, delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[server] += delta
}

func (s *State) Sync(server ServerId, count int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if count > s.counts[server] {
		s.counts[server] = count
	}
	return s.counts[server]
}

func (s *State) Read() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := 0
	for _, count := range s.counts {
		total += count
	}
	return total
}

func (s *State) ReadSubCount(server ServerId) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.counts[server]
}

type Ack struct {
	From  ServerId
	Count int
}

func runPeriodicSync(
	ctx context.Context,
	node *maelstrom.Node,
	state *State,
	ackCh <-chan Ack,
) error {
	acks := map[ServerId]int{}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ack := <-ackCh:
			if ack.Count > acks[ack.From] {
				acks[ack.From] = ack.Count
			}
		case <-ticker.C:
			currCount := state.ReadSubCount(node.ID())
			for _, id := range node.NodeIDs() {
				if id == node.ID() {
					continue
				}
				ackedCount := acks[id]
				if currCount > ackedCount {
					syncMsg := SyncMsg{
						Type:  "sync",
						Count: currCount,
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
		counts: map[ServerId]int{},
	}
	ackCh := make(chan Ack, 1024)

	n.Handle("add", handleAdd(n, &state))
	n.Handle("read", handleRead(n, &state))
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
// Add Handler
// ---------------------

type AddMsg struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type AddAck struct {
	Type string `json:"type"`
}

func handleAdd(node *maelstrom.Node, state *State) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body AddMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.Increment(node.ID(), body.Delta)
		ack := AddAck{
			Type: "add_ok",
		}
		return node.Reply(msg, ack)
	}
}

// ---------------------
// Read Handler
// ---------------------

type ReadMsg struct {
	Type string `json:"type"`
}

type ReadAck struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func handleRead(node *maelstrom.Node, state *State) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body ReadMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		count := state.Read()
		ack := ReadAck{
			Type:  "read_ok",
			Value: count,
		}
		return node.Reply(msg, ack)

	}
}

// ---------------------
// Sync Handlers
// ---------------------

type SyncMsg struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

type SyncAck struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

func handleSync(node *maelstrom.Node, state *State) maelstrom.HandlerFunc {
	return func(msg maelstrom.Message) error {
		var body SyncMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		newCount := state.Sync(msg.Src, body.Count)
		ack := SyncAck{
			Type:  "sync_ok",
			Count: newCount,
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
		case ackCh <- Ack{From: msg.Src, Count: body.Count-}:
		default:
			log.Printf("WARN: dropping ack from %s (channel full)", msg.Src)
		}
		return nil
	}
}
