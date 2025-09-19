package main

import (
	"fmt"
	"sync"
)

type State struct {
	mu      sync.RWMutex
	logs    map[LogKey][]int
	commits map[LogKey]int
}

func NewState() *State {
	return &State{
		logs:    map[LogKey][]int{},
		commits: map[LogKey]int{},
	}
}

func (s *State) AddMessage(logKey LogKey, msg int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgIdx := len(s.logs[logKey])
	s.logs[logKey] = append(s.logs[logKey], msg)
	return msgIdx
}

func (s *State) Commit(logKey LogKey, committed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if committed > s.commits[logKey] {
		s.commits[logKey] = committed
	}
}

func (s *State) Sync(logKey LogKey, syncIdx int, syncSuffix []int, committed int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	currLogLen := len(s.logs[logKey])
	if syncIdx < 0 || syncIdx > currLogLen {
		return currLogLen, fmt.Errorf("Invalid suffix sync: logLen=%v, syncIdx=%v", currLogLen, syncIdx)
	}
	newElements := syncSuffix[currLogLen-syncIdx:]
	s.logs[logKey] = append(s.logs[logKey], newElements...)
	if committed > s.commits[logKey] {
		s.commits[logKey] = committed
	}
	return len(s.logs[logKey]), nil
}

func (s *State) GetSuffix(logKey LogKey, from int) ([]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	log := s.logs[logKey]
	if from < 0 || from > len(log) {
		return nil, fmt.Errorf("WARN: Tried to get non-sensical suffix: %v..%v for keyHash %v", from, len(log), logKey)
	} else if from == len(log) {
		return nil, nil
	}
	suffix := make([]int, len(log)-from)
	copy(suffix, log[from:])
	return suffix, nil
}

func (s *State) PollSuffix(logKey LogKey, from int) [][]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	log := s.logs[logKey]
	if from < 0 || from >= len(log) {
		return [][]int{}
	}
	poll := make([][]int, 0, len(log)-from)
	for offset := from; offset < len(log); offset++ {
		poll = append(poll, []int{offset, log[offset]})
	}
	return poll
}

func (s *State) GetCommit(logKey LogKey) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if commit, exists := s.commits[logKey]; exists {
		return commit
	} else {
		return -1
	}
}

func (s *State) GetLogKeys() []LogKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]LogKey, 0, len(s.logs))
	for k := range s.logs {
		keys = append(keys, k)
	}
	return keys
}
