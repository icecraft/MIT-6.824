package kvraft

import (
	"strconv"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second

func TestBasic3A(t *testing.T) {
	// Test: one client (3A) ...
	GenericTest(t, "3A", 1, false, false, false, -1)
}

func TestConcurrent3A(t *testing.T) {
	// Test: many clients (3A) ...
	GenericTest(t, "3A", 5, false, false, false, -1)
}

func TestUnreliable3A(t *testing.T) {
	// Test: unreliable net, many clients (3A) ...
	GenericTest(t, "3A", 5, true, false, false, -1)
}

func TestUnreliableOneKey3A(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, true, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: concurrent append to same key, unreliable (3A)")

	Put(cfg, ck, "k", "")

	const nclient = 5
	const upto = 10
	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
		n := 0
		for n < upto {
			Append(cfg, myck, "k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
			n++
		}
	})

	var counts []int
	for i := 0; i < nclient; i++ {
		counts = append(counts, upto)
	}

	vx := Get(cfg, ck, "k")
	checkConcurrentAppends(t, vx, counts)

	cfg.end()
}

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
func TestOnePartition3A(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()

	// ck can connect with all other servers
	ck := cfg.makeClient(cfg.All())

	Put(cfg, ck, "1", "13")

	cfg.begin("Test: progress in majority (3A)")

	// Create a partition, the current leader is in the minority
	// cfg.partition sets up the connection to match the paritition.
	// p1, p2: []int, p1 is majority, p2 is minority
	// The current leader is in p2
	p1, p2 := cfg.make_partition()
	cfg.partition(p1, p2)

	ckp1 := cfg.makeClient(p1)  // connect ckp1 to p1
	ckp2a := cfg.makeClient(p2) // connect ckp2a to p2
	ckp2b := cfg.makeClient(p2) // connect ckp2b to p2

	Put(cfg, ckp1, "1", "14")
	check(cfg, t, ckp1, "1", "14")

	cfg.end()

	done0 := make(chan bool)
	done1 := make(chan bool)

	cfg.begin("Test: no progress in minority (3A)")
	go func() {
		Put(cfg, ckp2a, "1", "15")
		done0 <- true
	}()
	go func() {
		Get(cfg, ckp2b, "1") // different clerk in p2
		done1 <- true
	}()

	select {
	case <-done0:
		t.Fatalf("Put in minority completed")
	case <-done1:
		t.Fatalf("Get in minority completed")
	case <-time.After(time.Second):
	}

	check(cfg, t, ckp1, "1", "14")
	Put(cfg, ckp1, "1", "16")
	check(cfg, t, ckp1, "1", "16")

	cfg.end()

	cfg.begin("Test: completion after heal (3A)")

	cfg.ConnectAll()
	cfg.ConnectClient(ckp2a, cfg.All())
	cfg.ConnectClient(ckp2b, cfg.All())

	time.Sleep(electionTimeout)

	select {
	case <-done0:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Put did not complete")
	}

	select {
	case <-done1:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Get did not complete")
	default:
	}

	check(cfg, t, ck, "1", "15")

	cfg.end()
}

func TestManyPartitionsOneClient3A(t *testing.T) {
	// Test: partitions, one client (3A) ...
	GenericTest(t, "3A", 1, false, false, true, -1)
}

func TestManyPartitionsManyClients3A(t *testing.T) {
	// Test: partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, false, false, true, -1)
}

func TestPersistOneClient3A(t *testing.T) {
	// Test: restarts, one client (3A) ...
	GenericTest(t, "3A", 1, false, true, false, -1)
}

func TestPersistConcurrent3A(t *testing.T) {
	// Test: restarts, many clients (3A) ...
	GenericTest(t, "3A", 5, false, true, false, -1)
}

func TestPersistConcurrentUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, many clients (3A) ...
	GenericTest(t, "3A", 5, true, true, false, -1)
}

func TestPersistPartition3A(t *testing.T) {
	// Test: restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, false, true, true, -1)
}

func TestPersistPartitionUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, true, true, true, -1)
}

func TestPersistPartitionUnreliableLinearizable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, linearizability checks (3A) ...
	GenericTestLinearizability(t, "3A", 15, 7, true, true, true, -1)
}

//
// if one server falls behind, then rejoins, does it
// recover by using the InstallSnapshot RPC?
// also checks that majority discards committed log entries
// even if minority doesn't respond.
//
func TestSnapshotRPC3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: InstallSnapshot RPC (3B)")

	Put(cfg, ck, "a", "A")
	check(cfg, t, ck, "a", "A")

	// a bunch of puts into the majority partition.
	cfg.partition([]int{0, 1}, []int{2})
	{
		ck1 := cfg.makeClient([]int{0, 1})
		for i := 0; i < 50; i++ {
			Put(cfg, ck1, strconv.Itoa(i), strconv.Itoa(i))
		}
		time.Sleep(electionTimeout)
		Put(cfg, ck1, "b", "B")
	}

	// check that the majority partition has thrown away
	// most of its log entries.
	sz := cfg.LogSize()
	if sz > 8*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// now make group that requires participation of
	// lagging server, so that it has to catch up.
	cfg.partition([]int{0, 2}, []int{1})
	{
		ck1 := cfg.makeClient([]int{0, 2})
		Put(cfg, ck1, "c", "C")
		Put(cfg, ck1, "d", "D")
		check(cfg, t, ck1, "a", "A")
		check(cfg, t, ck1, "b", "B")
		check(cfg, t, ck1, "1", "1")
		check(cfg, t, ck1, "49", "49")
	}

	// now everybody
	cfg.partition([]int{0, 1, 2}, []int{})

	Put(cfg, ck, "e", "E")
	check(cfg, t, ck, "c", "C")
	check(cfg, t, ck, "e", "E")
	check(cfg, t, ck, "1", "1")

	cfg.end()
}

// are the snapshots not too huge? 500 bytes is a generous bound for the
// operations we're doing here.
func TestSnapshotSize3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	maxsnapshotstate := 500
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: snapshot size is reasonable (3B)")

	for i := 0; i < 200; i++ {
		Put(cfg, ck, "x", "0")
		check(cfg, t, ck, "x", "0")
		Put(cfg, ck, "x", "1")
		check(cfg, t, ck, "x", "1")
	}

	// check that servers have thrown away most of their log entries
	sz := cfg.LogSize()
	if sz > 8*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// check that the snapshots are not unreasonably large
	ssz := cfg.SnapshotSize()
	if ssz > maxsnapshotstate {
		t.Fatalf("snapshot too large (%v > %v)", ssz, maxsnapshotstate)
	}

	cfg.end()
}

func TestSnapshotRecover3B(t *testing.T) {
	// Test: restarts, snapshots, one client (3B) ...
	GenericTest(t, "3B", 1, false, true, false, 1000)
}

func TestSnapshotRecoverManyClients3B(t *testing.T) {
	// Test: restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 20, false, true, false, 1000)
}

func TestSnapshotUnreliable3B(t *testing.T) {
	// Test: unreliable net, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, false, false, 1000)
}

func TestSnapshotUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, false, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartition3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, true, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, linearizability checks (3B) ...
	GenericTestLinearizability(t, "3B", 15, 7, true, true, true, 1000)
}
