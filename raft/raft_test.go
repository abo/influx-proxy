// Copyright 2023 Shengbo Huang. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package raft

import (
	"reflect"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestProcessMessages(t *testing.T) {
	cases := []struct {
		name             string
		confState        raftpb.ConfState
		InputMessages    []raftpb.Message
		ExpectedMessages []raftpb.Message
	}{
		{
			name: "only one snapshot message",
			confState: raftpb.ConfState{
				Voters: []uint64{2, 6, 8, 10},
			},
			InputMessages: []raftpb.Message{
				{
					Type: raftpb.MsgSnap,
					To:   8,
					Snapshot: raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: 100,
							Term:  3,
							ConfState: raftpb.ConfState{
								Voters:    []uint64{2, 6, 8},
								AutoLeave: true,
							},
						},
					},
				},
			},
			ExpectedMessages: []raftpb.Message{
				{
					Type: raftpb.MsgSnap,
					To:   8,
					Snapshot: raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: 100,
							Term:  3,
							ConfState: raftpb.ConfState{
								Voters: []uint64{2, 6, 8, 10},
							},
						},
					},
				},
			},
		},
		{
			name: "one snapshot message and one other message",
			confState: raftpb.ConfState{
				Voters: []uint64{2, 7, 8, 12},
			},
			InputMessages: []raftpb.Message{
				{
					Type: raftpb.MsgSnap,
					To:   8,
					Snapshot: raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: 100,
							Term:  3,
							ConfState: raftpb.ConfState{
								Voters:    []uint64{2, 6, 8},
								AutoLeave: true,
							},
						},
					},
				},
				{
					Type: raftpb.MsgApp,
					From: 6,
					To:   8,
				},
			},
			ExpectedMessages: []raftpb.Message{
				{
					Type: raftpb.MsgSnap,
					To:   8,
					Snapshot: raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: 100,
							Term:  3,
							ConfState: raftpb.ConfState{
								Voters: []uint64{2, 7, 8, 12},
							},
						},
					},
				},
				{
					Type: raftpb.MsgApp,
					From: 6,
					To:   8,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rn := &raftNode{
				confState: tc.confState,
			}

			outputMessages := rn.processMessages(tc.InputMessages)

			if !reflect.DeepEqual(outputMessages, tc.ExpectedMessages) {
				t.Fatalf("Unexpected messages, expected: %v, got %v", tc.ExpectedMessages, outputMessages)
			}
		})
	}
}

// func TestIt(t *testing.T) {
// 	start := time.Now()
// 	fmt.Println(test(5))
// 	fmt.Println(time.Now().Sub(start))

// 	time.Sleep(5 * time.Second)
// }

// func anyActive(l int) bool {
// 	count := int32(0)
// 	ch := make(chan bool, l)
// 	for i := 1; i <= l; i++ {

// 		go func(baseurl int) {
// 			fmt.Println("start ", baseurl)
// 			time.Sleep(time.Second * time.Duration(baseurl))
// 			ch <- baseurl%2 == 0
// 			// r, e := client.Get(fmt.Sprintf("%s%s", baseurl, rafthttp.ProbingPrefix))
// 			// ch <- e != nil && r.StatusCode == http.StatusOK
// 			if atomic.AddInt32(&count, 1) == int32(l) {
// 				fmt.Println("close in ", baseurl)
// 				close(ch)
// 			} else {
// 				fmt.Println(baseurl, " done")
// 			}
// 		}(i)
// 	}

// 	for active := range ch {
// 		if active {
// 			return true
// 		}
// 	}
// 	return false
// }
