package kvraft

import "testing"

func TestServerGet(t *testing.T) {
	cfg := make_config(
		/* tester= */ t,
		/* nservers= */ 3,
		/* unreliable= */ false,
		/* maxraftstate= */ -1,
		/* frozen= */ true)
}