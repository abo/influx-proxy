package sharding

import (
	"log"
	"testing"
)

// func TestIt(t *testing.T) {
// 	ms := []string{"cpe.cpe_phy_priority_rxtx",
// 		"cpe.cpe_phy_rxtx",
// 		"cpe.shared_subscription_rxtx",
// 		"cpe.subscription_rxtx"}
// 	for _, m := range ms {
// 		fmt.Println(jumpHashForReplica(str2key(m), 0, 2))
// 	}
// 	t.FailNow()
// }

func TestHash(t *testing.T) {
	count := 100000
	replicas := 2
	// 2 node 2 replica 时, 任一 node 中应该有完整数据
	nodes := map[int32]map[uint64]struct{}{
		int32(0): make(map[uint64]struct{}),
		int32(1): make(map[uint64]struct{})}
	for i := uint64(0); i < uint64(count); i++ {
		assigns := jumpHashForReplica(i, replicas-1, int32(len(nodes)))
		nodes[assigns[0]][i] = struct{}{}
		nodes[assigns[1]][i] = struct{}{}
	}

	if len(nodes[0]) != count || len(nodes[1]) != count {
		t.FailNow()
	}

	// 增加一个节点时
	nodes[int32(2)] = make(map[uint64]struct{})
	for i := uint64(0); i < uint64(count); i++ {
		assigns := jumpHashForReplica(i, replicas-1, int32(len(nodes)))
		if assigns[0] == 2 || assigns[1] == 2 {
			// moved to node2
			nodes[2][i] = struct{}{}
			if assigns[0] == 1 || assigns[1] == 1 {
				delete(nodes[0], i)
			} else {
				delete(nodes[1], i)
			}
		}
	}
	log.Printf("node-0: %d, node-1: %d, node-2: %d", len(nodes[0]), len(nodes[1]), len(nodes[2]))

}
