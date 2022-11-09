package raft

import "testing"

func TestLogLowerTerm(t *testing.T) {
	rf := &Raft{
		log: []Entry{
			Entry{
				Term: 1,
			},
			Entry{
				Term: 1,
			},
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
			Entry{
				Term: 1,
			},
			Entry{
				Term: 1,
			},
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
			Entry{
				Term: 1,
			},
			Entry{
				Term: 1,
			},
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
			Entry{
				Term: 1,
			},
			Entry{
				Term: 1,
			},
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
			Entry{
				Term: 1,
			},
			Entry{
				Term: 1,
			},
			Entry{
				Term: 2,
			},
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



