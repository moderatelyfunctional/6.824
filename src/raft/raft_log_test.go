package raft

// import "fmt"
import "testing"

func checkLog(log *Log, startIndex int, numEntries int, snapshotTerm int, t *testing.T) {
	if log.startIndex != startIndex {
		t.Errorf("checkLog startIndex expected %d, got %d", startIndex, log.startIndex)
	}
	if len(log.entries) != numEntries {
		t.Errorf("checkLog numEntries expected %d, got %d", numEntries, len(log.entries))
	}
	if log.snapshotTerm != snapshotTerm {
		t.Errorf("checkLog snapshotTerm expected %d, got %d", snapshotTerm, log.snapshotTerm)
	}
}

func TestLogSetLogIndex(t *testing.T) {
	log := &Log{
		startIndex: 2,
		snapshotTerm: 1,
		entries: []Entry{
			Entry{Term: 2,},
			Entry{Term: 3,},
		},
	}
	log.compact(2)
	checkLog(
		log,
		/* startIndex= */ 3, 
		/* numEntries= */ 1,
		/* snapshotLogTerm= */ 2,
		t)

	log.compact(3)
	checkLog(
		log,
		/* startIndex= */ 4,
		/* numEntries= */ 0,
		/* snapshotLogTerm= */ 3,
		t)
	log.compact(4)
	checkLog(
		log,
		/* startIndex= */ 4,
		/* numEntries= */ 0,
		/* snapshotLogTerm= */ 3,
		t)
	log.compact(5)
	checkLog(
		log,
		/* startIndex= */ 4,
		/* numEntries= */ 0,
		/* snapshotLogTerm= */ 3,
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
		/* snapshotLogTerm= */ 0,
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
		/* snapshotLogTerm= */ 0,
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
		/* snapshotLogTerm= */ 0,
		t)
}

func TestLogTerm(t *testing.T) {
	log := &Log{
		startIndex: 0,
		entries: []Entry{
			Entry{Term: 1, Command: 'A',},
			Entry{Term: 1, Command: 'B',},
			Entry{Term: 1, Command: 'D',},
		},
	}

	if !log.isMoreUpToDate(/* otherLastLogIndex= */ 1, /* otherLastLogTerm= */ 1) {
		t.Errorf("TestLogTerm otherLastLogIndex=1 otherLastLogTerm=1 expected True")
	}
	if log.isMoreUpToDate(/* otherLastLogIndex= */ 1, /* otherLastLogTerm= */ 5) {
		t.Errorf("TestLogTerm otherLastLogIndex=1 otherLastLogTerm=5 expected False")
	}
	if log.isMoreUpToDate(/* otherLastLogIndex= */ 4, /* otherLastLogTerm= */ 1) {
		t.Errorf("TestLogTerm otherLastLogIndex=4 otherLastLogTerm=1 expected False")
	}
	log.compact(3)
	if !log.isMoreUpToDate(/* otherLastLogIndex= */ 1, /* otherLastLogTerm= */ 1) {
		t.Errorf("TestLogTerm otherLastLogIndex=2 otherLastLogTerm=1 expected True")
	}
}

