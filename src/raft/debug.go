package raft

import "fmt"
import "log"
import "time"

type logTopic string
const (
	dElection	logTopic = "ELEC"
	dHeart 		logTopic = "HART"
	dInfo 		logTopic = "INFO"
	dLeader		logTopic = "LEAD"
	dTimer		logTopic = "TIMR"
	dVote 		logTopic = "VOTE"
)

var debugStart time.Time
func setupDebug() {
	if (!debugStart.IsZero()) {
		return
	}
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// Debugging
const Debug = true
func DPrintf(topic logTopic, format string, a ...interface{}) {
	if Debug {
		debugNow := int64(time.Since(debugStart) / time.Millisecond)
		prefix := fmt.Sprintf("%06d %v", debugNow, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}