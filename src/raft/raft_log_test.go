package raft

import "testing"

func checkLog(log *Log, logStartIndex int, lastLogTerm int, numEntries int, t *testing.T) {
	if log.logStartIndex != logStartIndex {
		t.Errorf("checkLog logStartIndex expected %d, got %d", logStartIndex, log.logStartIndex)
	}
	if log.lastLogTerm != lastLogTerm {
		t.Errorf("checkLog lastLogTerm expected %d, got %d", lastLogTerm, log.lastLogTerm)
	}
	if len(log.entries) != numEntries {
		t.Errorf("checkLog numEntries expected %d, got %d", numEntries, len(log.entries))
	}
}

func TestLogSetLogIndex(t *testing.T) {
	log := &Log{
		logStartIndex: 2,
		lastLogTerm: 3,
		entries: []Entry{
			Entry{Term: 1,},
			Entry{Term: 3,},
		},
	}
	log.setStartIndex(2)
	checkLog(
		log,
		/* logStartIndex= */ 2, 
		/* lastLogTerm= */ 3,
		/* numEntries= */ 2,
		t)

	log.setStartIndex(4)
	checkLog(
		log,
		/* logStartIndex= */ 4,
		/* lastLogTerm= */ 3,
		/* numEntries= */ 0,
		t)
}

func TestLogCheckEntry(t *testing.T) {
	log := &Log{
		logStartIndex: 2,
	}
	if log.checkEntry(1, 1) {
		t.Errorf("TestCheckEntry for (Index: 1 Term: 1) expected conflict")
	}
	if log.checkEntry(2, 1) {
		t.Errorf("TestCheckEntry for (Index: 2 Term: 1) expected conflict")
	}
	if log.checkEntry(3, 1) {
		t.Errorf("TestCheckEntry for (Index: 3 Term: 1) expected conflict")	
	}
	log.entries = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
	}
	if !(log.checkEntry(2, 1) && log.checkEntry(3, 1)) {
		t.Errorf("TestCheckEntry expected agreement for (Index: 2 Term: 1), (Index: 3, Term: 1)")
	}
	if log.checkEntry(3, 2) {
		t.Errorf("TestCheckEntry for (Index: 3 Term: 2) expected conflict")	
	}
}

func TestLogAppend(t *testing.T) {
	log := &Log{
		logStartIndex: 0,
		lastLogTerm: -1,
	}

	log.append([]Entry{})
	checkLog(
		log,
		/* logStartIndex= */ 0,
		/* lastLogTerm= */ -1,
		/* numEntries= */ 0,
		t)
}

func TestLogLowerTerm(t *testing.T) {
	rf := &Raft{
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
		},
	}
	otherLastLogIndex := 1
	otherLastLogTerm := 2
	if rf.isLogMoreUpToDate(otherLastLogIndex, otherLastLogTerm) {
		t.Errorf(
			"TetLogLowerTerm for %#v with other index %d and term %d returns True, expected False",
			rf,
			otherLastLogIndex,
			otherLastLogTerm,
		)
	}
}

func TestLogSameTermShorterLog(t *testing.T) {
	rf := &Raft{
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
		},
	}
	otherLastLogIndex := 5
	otherLastLogTerm := 1
	if rf.isLogMoreUpToDate(otherLastLogIndex, otherLastLogTerm) {
		t.Errorf(
			"TestLogSameTermShorterLog for %#v with other index %d and term %d returns True, expected False",
			rf,
			otherLastLogIndex,
			otherLastLogTerm,
		)
	}
}

func TestLogSameTermEqualLog(t *testing.T) {
	rf := &Raft{
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
		},
	}
	otherLastLogIndex := 1
	otherLastLogTerm := 1
	if rf.isLogMoreUpToDate(otherLastLogIndex, otherLastLogTerm) {
		t.Errorf(
			"TestLogSameTermEqualLog for %#v with other index %d and term %d returns True, expected False",
			rf,
			otherLastLogIndex,
			otherLastLogTerm,
		)
	}
}

func TestLogSameTermLongerLog(t *testing.T) {
	rf := &Raft{
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
		},
	}
	otherLastLogIndex := 0
	otherLastLogTerm := 1
	if !rf.isLogMoreUpToDate(otherLastLogIndex, otherLastLogTerm) {
		t.Errorf(
			"TestLogSameTermLongerLog for %#v with other index %d and term %d returns False, expected True",
			rf,
			otherLastLogIndex,
			otherLastLogTerm,
		)
	}
}

func TestLogHigherTerm(t *testing.T) {
	rf := &Raft{
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
			Entry{Term: 2,},
		},
	}
	otherLastLogIndex := 1
	otherLastLogTerm := 1
	if !rf.isLogMoreUpToDate(otherLastLogIndex, otherLastLogTerm) {
		t.Errorf(
			"TestLogHigherTerm for %#v with other index %d and term %d returns False, expected True",
			rf,
			otherLastLogIndex,
			otherLastLogTerm,
		)
	}
}



