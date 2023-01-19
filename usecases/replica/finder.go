//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replica

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

// Finder finds replicated objects
type Finder struct {
	RClient            // needed to commit and abort operation
	resolver *resolver // host names of replicas
	class    string
}

func NewFinder(className string,
	stateGetter shardingState, nodeResolver nodeResolver,
	client RClient,
) *Finder {
	return &Finder{
		class: className,
		resolver: &resolver{
			schema:       stateGetter,
			nodeResolver: nodeResolver,
			class:        className,
		},
		RClient: client,
	}
}

// FindOne finds one object which satisfies the giving consistency
func (f *Finder) FindOne(ctx context.Context, l ConsistencyLevel, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties,
) (*storobj.Object, error) {
	c := newReadCoordinator[findOneReply](f, shard)
	op := func(ctx context.Context, host string) (findOneReply, error) {
		obj, err := f.FindObject(ctx, host, f.class, shard, id, props, additional)
		return findOneReply{host, obj}, err
	}
	replyCh, level, err := c.Fetch(ctx, l, op)
	if err != nil {
		return nil, err
	}

	// var level int
	// state, err := f.resolver.State(shard)
	// if err == nil {
	// 	level, err = state.ConsistencyLevel(l)
	// }
	// if err != nil {
	// 	return nil, fmt.Errorf("%w : class %q shard %q", err, f.class, shard)
	// }

	// ctx, cancel := context.WithCancel(ctx)
	// defer cancel()

	// writer := func() <-chan tuple {
	// 	responses := make(chan tuple, len(state.Hosts))
	// 	var g errgroup.Group
	// 	for i, host := range state.Hosts {
	// 		i, host := i, host
	// 		g.Go(func() error {
	// 			o, err := f.FindObject(ctx, host, f.class, shard, id, props, additional)
	// 			responses <- tuple{o, i, err}
	// 			return nil
	// 		})
	// 	}
	// 	go func() { g.Wait(); close(responses) }()
	// 	return responses
	// }

	return readObject(replyCh, level)
}

// NodeObject gets object from a specific node.
// it is used mainly for debugging purposes
func (f *Finder) NodeObject(ctx context.Context, nodeName, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties,
) (*storobj.Object, error) {
	host, ok := f.resolver.NodeHostname(nodeName)
	if !ok || host == "" {
		return nil, fmt.Errorf("cannot resolve node name: %s", nodeName)
	}
	return f.RClient.FindObject(ctx, host, f.class, shard, id, props, additional)
}

func readObject(ch <-chan simpleResult[findOneReply], cl int) (*storobj.Object, error) {
	counters := make([]tuple, 0, cl*2)
	nnf := 0
	N := 0
	for r := range ch {
		N++
		resp := r.Response
		if r.Err != nil {
			counters = append(counters, tuple{resp.sender, nil, 0, r.Err})
			continue
		} else if resp.data == nil {
			nnf++
			continue
		}
		// counters[r.sender] = tuple{r.o, 1, nil}
		counters = append(counters, tuple{resp.sender, resp.data, 0, nil})
		lastTime := resp.data.LastUpdateTimeUnix()
		max := 0
		for i := range counters {
			if counters[i].o != nil && counters[i].o.LastUpdateTimeUnix() == lastTime {
				counters[i].ack++
			}
			if max < counters[i].ack {
				max = counters[i].ack
			}
			if max >= cl {
				return counters[i].o, nil
			}
		}
	}
	if nnf == N { // object doesn't exist
		return nil, nil
	}

	var sb strings.Builder
	for i, c := range counters {
		if i != 0 {
			sb.WriteString(", ")
		}
		if c.err != nil {
			fmt.Fprintf(&sb, "%s: %s", c.sender, c.err.Error())
		} else if c.o == nil {
			fmt.Fprintf(&sb, "%s: 0", c.sender)
		} else {
			fmt.Fprintf(&sb, "%s: %d", c.sender, c.o.LastUpdateTimeUnix())
		}
	}
	return nil, errors.New(sb.String())
}

type tuple struct {
	sender string
	o      *storobj.Object
	ack    int
	err    error
}

type senderReply[T any] struct {
	sender string
	data   T
}

type findOneReply senderReply[*storobj.Object]
