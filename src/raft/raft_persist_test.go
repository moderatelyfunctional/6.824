package raft

import "reflect"
import "testing"

func TestPersistStoresAndRetrieveData(t *testing.T) {
	rf := &Raft{
		me: 0,
		currentTerm: 2,
		votedFor: 0,
		votesReceived: []int{1, 1, 0},
		commitIndex: 3,
		lastApplied: 3,
		log: makeLogFromSnapshot(
			/* startIndex= */ 4,
			/* snapshotTerm= */ 1,
			/* snapshotIndex= */ 3,
			/* entries= */ []Entry{
				Entry{Term: 1,},
				Entry{Term: 1,},
				Entry{Term: 2,},
			},
		),
		persister: MakePersister(),
	}
	rf.persist()
	persister := rf.persister

	other := &Raft{log: makeLog([]Entry{})}
	other.readPersist(persister.ReadRaftState())

	rf.persister = nil
	other.persister = nil
	if (!reflect.DeepEqual(rf, other)) {
		t.Errorf("TestRaftPersistStoresAndRetrieveData rf %s not equal to other %s", rf.prettyPrint(), other.prettyPrint())
	}
}