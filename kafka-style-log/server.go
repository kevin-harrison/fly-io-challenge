package main

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ServerName = string
type ServerID = int
type LogKey = string

type Server struct {
	name     ServerName
	peerName ServerName
	id       ServerID
	node     *maelstrom.Node

	state *State

	// background sync
	syncCtx    context.Context
	syncCancel context.CancelFunc
	ackCh      chan Ack
	wg         sync.WaitGroup
}

func NewServer(node *maelstrom.Node) *Server {
	state := NewState()
	ackCh := make(chan Ack, 1024)
	server := &Server{
		name:  "unknown",
		id:    -1,
		node:  node,
		state: state,
		ackCh: ackCh,
	}

	node.Handle("init", server.handleInit)
	node.Handle("send", server.handleSend)
	node.Handle("poll", server.handlePoll)
	node.Handle("commit_offsets", server.handleCommit)
	node.Handle("list_committed_offsets", server.handleListCommit)
	node.Handle("sync", server.handleSync)
	node.Handle("sync_ok", server.handleSyncOk)

	return server
}

func (s *Server) Run() error {
	return s.node.Run()
}

func (s *Server) ownsKey(key LogKey) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()%uint32(2)) == s.id
}

// ---------------------
// Init Handler
// ---------------------

func GetServerID(serverName ServerName) ServerID {
	id, err := strconv.Atoi(serverName[1:])
	if err != nil {
		log.Fatal(err)
	}
	return id
}

type InitMsg struct {
	Type    string   `json:"type"`
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

func (s *Server) handleInit(msg maelstrom.Message) error {
	var body InitMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.name = body.NodeID
	s.id = GetServerID(s.name)
	var peerName ServerName
	for _, name := range body.NodeIDs {
		if name != s.name {
			peerName = name
			break
		}
	}
	s.peerName = peerName

	// Start periodic sync goroutine
	s.syncCtx, s.syncCancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.runPeriodicSync(s.syncCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("sync loop ended with error: %v", err)
		}
	}()

	return nil
}

// ---------------------
// Send Handler
// ---------------------

type SendMsg struct {
	Type    string `json:"type"`
	Key     LogKey `json:"key"`
	Message int    `json:"msg"`
}

type SendAck struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

func (s *Server) handleSend(msg maelstrom.Message) error {
	var body SendMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// TODO: remove
	if s.id == -1 {
		log.Fatal("got messages before init set id")
	}

	if s.ownsKey(body.Key) {
		// If I own log, append and reply directly
		offset := s.state.AddMessage(body.Key, body.Message)
		ack := SendAck{
			Type:   "send_ok",
			Offset: offset,
		}
		return s.node.Reply(msg, ack)
	} else {
		// If peer owns log, proxy msg and response of peer
		return s.node.RPC(s.peerName, body, func(reply maelstrom.Message) error {
			var ack SendAck
			if err := json.Unmarshal(reply.Body, &ack); err != nil {
				return err
			}
			return s.node.Reply(msg, ack)
		})
	}
}

// ---------------------
// Poll Handler
// ---------------------

type PollMsg struct {
	Type    string         `json:"type"`
	Offsets map[LogKey]int `json:"offsets"`
}

type PollAck struct {
	Type     string             `json:"type"`
	Messages map[LogKey][][]int `json:"msgs"`
}

func (s *Server) handlePoll(msg maelstrom.Message) error {
	var body PollMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Instant read from own state and reply
	suffixes := map[LogKey][][]int{}
	for key, offset := range body.Offsets {
		suffix := s.state.PollSuffix(key, offset)
		suffixes[key] = suffix
	}
	ack := PollAck{
		Type:     "poll_ok",
		Messages: suffixes,
	}
	return s.node.Reply(msg, ack)
}

// ---------------------
// Commit Handler
// ---------------------

type CommitMsg struct {
	Type    string         `json:"type"`
	Offsets map[LogKey]int `json:"offsets"`
}

type CommitAck struct {
	Type string `json:"type"`
}

func (s *Server) handleCommit(msg maelstrom.Message) error {
	var body CommitMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// For keys I own, update directly
	for key, offset := range body.Offsets {
		if s.ownsKey(key) {
			s.state.Commit(key, offset)
			delete(body.Offsets, key)
		}
	}
	if len(body.Offsets) == 0 {
		ack := CommitAck{
			Type: "commit_offsets_ok",
		}
		return s.node.Reply(msg, ack)
	}

	// Proxy rest of offsets and reply of peer
	return s.node.RPC(s.peerName, body, func(reply maelstrom.Message) error {
		var ack CommitAck
		if err := json.Unmarshal(reply.Body, &ack); err != nil {
			return err
		}
		return s.node.Reply(msg, ack)
	})
}

// ---------------------
// List Commit Handler
// ---------------------

type ListCommitMsg struct {
	Type string   `json:"type"`
	Keys []LogKey `json:"keys"`
}

type ListCommitAck struct {
	Type    string         `json:"type"`
	Offsets map[LogKey]int `json:"offsets"`
}

func (s *Server) handleListCommit(msg maelstrom.Message) error {
	var body ListCommitMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	myCommits := map[LogKey]int{}
	peers_keys := []LogKey{}

	// For keys I own, read directly
	for _, key := range body.Keys {
		if s.ownsKey(key) {
			commit := s.state.GetCommit(key)
			if commit != -1 {
				myCommits[key] = commit
			}
		} else {
			peers_keys = append(peers_keys, key)
		}
	}
	if len(peers_keys) == 0 {
		ack := ListCommitAck{
			Type:    "list_committed_offsets_ok",
			Offsets: myCommits,
		}
		return s.node.Reply(msg, ack)
	}

	// Forward rest of offsets to other node then construct reply
	body.Keys = peers_keys
	return s.node.RPC(s.peerName, body, func(reply maelstrom.Message) error {
		var ack ListCommitAck
		if err := json.Unmarshal(reply.Body, &ack); err != nil {
			return err
		}
		for key, commit := range myCommits {
			ack.Offsets[key] = commit
		}
		return s.node.Reply(msg, ack)
	})
}

// ---------------------
// Sync Handlers
// ---------------------

type SyncMsg struct {
	Type     string             `json:"type"`
	LogSyncs map[LogKey]LogSync `json:"log_syncs"`
}

type SyncAck struct {
	Type    string         `json:"type"`
	LogAcks map[LogKey]int `json:"log_acks"`
}

func (s *Server) handleSync(msg maelstrom.Message) error {
	var body SyncMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	logAcks := LogAcks{}
	for logKey, sync := range body.LogSyncs {
		newLen, err := s.state.Sync(logKey, sync.SyncIdx, sync.SyncSuffix, sync.Committed)
		if err != nil {
			log.Fatal(err)
		}
		logAcks[logKey] = newLen
	}
	ack := SyncAck{
		Type:    "sync_ok",
		LogAcks: logAcks,
	}
	return s.node.Reply(msg, ack)
}

func (s *Server) handleSyncOk(msg maelstrom.Message) error {
	var body SyncAck
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	select {
	case s.ackCh <- Ack{msg.Src, body.LogAcks}:
	default:
		log.Printf("WARN: dropping ack from %s (channel full)", msg.Src)
	}
	return nil
}

// ---------------------
// Sync Runner
// ---------------------

type LogSync struct {
	SyncSuffix []int `json:"sync_suffix"`
	SyncIdx    int   `json:"sync_idx"`
	// NOTE: not really needed since we never use non-linearizable commits
	Committed int `json:"committed"`
}

type LogAcks = map[LogKey]int
type Ack struct {
	From    ServerName
	LogAcks LogAcks
}

func MergeAcks(acks LogAcks, newAcks LogAcks) {
	for logKey, offset := range newAcks {
		if offset > acks[logKey] {
			acks[logKey] = offset
		}
	}
}

func (s *Server) runPeriodicSync(ctx context.Context) error {
	peerAcks := make(LogAcks)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ack := <-s.ackCh:
			log.Printf("merging acks")
			MergeAcks(peerAcks, ack.LogAcks)
		case <-ticker.C:
			logSyncs := make(map[LogKey]LogSync)
			for _, logKey := range s.state.GetLogKeys() {
				if !s.ownsKey(logKey) {
					continue
				}
				syncIdx := peerAcks[logKey]
				log.Printf("%v syncidx=%v", logKey, syncIdx)
				syncSuffix, err := s.state.GetSuffix(logKey, syncIdx)
				if err != nil {
					log.Fatal(err)
				}
				committed := s.state.GetCommit(logKey)
				if syncSuffix != nil {
					logSyncs[logKey] = LogSync{
						SyncSuffix: syncSuffix,
						SyncIdx:    syncIdx,
						Committed:  committed,
					}
				}
			}
			if len(logSyncs) > 0 {
				syncMsg := SyncMsg{
					Type:     "sync",
					LogSyncs: logSyncs,
				}
				if err := s.node.Send(s.peerName, syncMsg); err != nil {
					log.Printf("WARN: Couldn't send sync message: %v", err)
				}
			}
		}
	}
}
