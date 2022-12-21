package raft

// import "fmt"
import "testing"

func checkLog(log *Log, startIndex int, numEntries int, t *testing.T) {
	if log.startIndex != startIndex {
		t.Errorf("checkLog startIndex expected %d, got %d", startIndex, log.startIndex)
	}
	if len(log.entries) != numEntries {
		t.Errorf("checkLog numEntries expected %d, got %d", numEntries, len(log.entries))
	}
}

func TestLogSetLogIndex(t *testing.T) {
	log := &Log{
		startIndex: 2,
		entries: []Entry{
			Entry{Term: 1,},
			Entry{Term: 3,},
		},
	}
	log.compactLog(2)
	checkLog(
		log,
		/* startIndex= */ 2, 
		/* numEntries= */ 2,
		t)

	log.compactLog(3)
	checkLog(
		log,
		/* startIndex= */ 3,
		/* numEntries= */ 1,
		t)
	log.compactLog(4)
	checkLog(
		log,
		/* startIndex= */ 4,
		/* numEntries= */ 0,
		t)
	log.compactLog(5)
	checkLog(
		log,
		/* startIndex= */ 4,
		/* numEntries= */ 0,
		t)
}

func TestLogAppendEntry(t *testing.T) {
	log := &Log{
		startIndex: 0,
	}

	log.appendEntry(Entry{Term: 1, Command: 1,})
	checkLog(
		log,
		/* startIndex= */ 0,
		/* numEntries= */ 1,
		t)
}

func TestLogCheckAppendCase(t *testing.T) {
	log := &Log{
		startIndex: 2,
		entries: []Entry{
			Entry{Term: 1, Command: 1,},
		},
	}
	caseOne := log.checkAppendCase(
		/* prevLogIndex= */ 1,
		/* prevLogTerm= */ 1,
		[]Entry{},
		/* isFirstIndex= */ true)
	if APPEND_STALE_REQUEST != caseOne {
		t.Errorf("TestLogCheckAppendCase expected %v got %v", APPEND_STALE_REQUEST, caseOne)
	}
	caseTwo := log.checkAppendCase(
		/* prevLogIndex= */ 3,
		/* prevLogTerm= */ 1,
		[]Entry{},
		/* isFirstIndex= */ true)
	if APPEND_REQUIRE_SNAPSHOT != caseTwo {
		t.Errorf("TestLogCheckAppendCase expected %v got %v", APPEND_REQUIRE_SNAPSHOT, caseTwo)
	}
	caseThree := log.checkAppendCase(
		/* prevLogIndex= */ 3,
		/* prevLogTerm= */ 1,
		[]Entry{},
		/* isFirstIndex= */ false)
	if APPEND_MISSING_ENTRY != caseThree {
		t.Errorf("TestLogCheckAppendCase expected %v got %v", APPEND_MISSING_ENTRY, caseThree)
	}
	caseFour := log.checkAppendCase(
		/* prevLogIndex= */ 2,
		/* prevLogTerm= */ 2,
		[]Entry{},
		/* isFirstIndex= */ false)
	if APPEND_CONFLICTING_ENTRY != caseFour {
		t.Errorf("TestLogCheckAppendCase expected %v got %v", APPEND_CONFLICTING_ENTRY, caseFour)
	}
	caseFive := log.checkAppendCase(
		/* prevLogIndex= */ 2,
		/* prevLogTerm= */ 1,
		[]Entry{},
		/* isFirstIndex= */ false)
	if APPEND_ADD_ENTRIES != caseFive {
		t.Errorf("TestLogCheckAppendCase expected %v got %v", APPEND_ADD_ENTRIES, caseFive)
	}
}

func TestLogAppendEntries(t *testing.T) {
	log := &Log{
		startIndex: 0,
		entries: []Entry{
			Entry{Term: 1, Command: 'A',},
			Entry{Term: 1, Command: 'B',},
			Entry{Term: 1, Command: 'D',},
		},
	}

	log.appendEntries(
		/* startIndex= */ 1,
		[]Entry{
			Entry{Term: 1, Command: 'C',},
		},
		/* currentTerm= */ 1)
	checkLog(
		log,
		/* startIndex= */ 0,
		/* numEntries= */ 3,
		t)
}

func TestLogAppendEntriesDeletesEntry(t *testing.T) {
	log := &Log{
		startIndex: 0,
		entries: []Entry{
			Entry{Term: 1, Command: 'A',},
			Entry{Term: 1, Command: 'B',},
			Entry{Term: 1, Command: 'D',},
		},
	}

	log.appendEntries(
		/* startIndex= */ 1,
		[]Entry{
			Entry{Term: 1, Command: 'C',},
		},
		/* currentTerm= */ 2)
	checkLog(
		log,
		/* startIndex= */ 0,
		/* numEntries= */ 2,
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



