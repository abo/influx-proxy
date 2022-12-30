package backend

import (
	"net/http"
	"sync/atomic"
	"testing"
)

func TestExecQuery(t *testing.T) {

	atomicBool := func(b bool) atomic.Value {
		var result atomic.Value
		result.Store(b)
		return result
	}

	nodes := []*Backend{
		{HttpBackend: &HttpBackend{Name: "b1", active: atomicBool(true), rewriting: atomicBool(false), transferIn: atomicBool(false), writeOnly: true}},
		{HttpBackend: &HttpBackend{Name: "b2", active: atomicBool(true), rewriting: atomicBool(true), transferIn: atomicBool(false), writeOnly: false}},
		{HttpBackend: &HttpBackend{Name: "b3", active: atomicBool(true), rewriting: atomicBool(false), transferIn: atomicBool(false), writeOnly: false}},
		{HttpBackend: &HttpBackend{Name: "b4", active: atomicBool(true), rewriting: atomicBool(true), transferIn: atomicBool(false), writeOnly: true}},
		{HttpBackend: &HttpBackend{Name: "b5", active: atomicBool(true), rewriting: atomicBool(false), transferIn: atomicBool(false), writeOnly: false}},
	}

	query := func(b *Backend, r *http.Request, w http.ResponseWriter) ([]byte, error) {
		return []byte(b.Name), nil
	}

	// 当只有 2 和 4 non-writing 时，优先在这俩节点上负载均衡
	delta := 0
	for i := 0; i < 50; i++ {
		body, err := execQuery(nil, nil, nodes, query)
		if string(body) == nodes[2].Name {
			delta++
		} else if string(body) == nodes[4].Name {
			delta--
		} else {
			t.Fatalf("hit non-active backend: %v, err: %v", string(body), err)
		}
	}
	if delta > 5 {
		t.Fatalf("not balance: %d", delta)
	}

	// 当 active 的节点都不是 non-writing 时，请求依然会转发到它们执行
	nodes[2].active.Store(false)
	nodes[4].active.Store(false)
	for i := 0; i < 50; i++ {
		body, err := execQuery(nil, nil, nodes, query)
		if string(body) == nodes[2].Name || string(body) == nodes[4].Name {
			t.Fatalf("body: %v, err: %v", string(body), err)
		}
	}
}
