package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

/*
  This example demonstrates an event pipeline that debounces (dedupes) events.
  The first stage generates a certain amount of events at a given interval. Keys for the
  events emitted will belong to a set as large as specified. The second stage groups
  these events by key, emitting a batch (set) after a time window has passed, or once
  the batch meets a specific size.
*/

const timeFmt = "[15:04:05]"

func main() {
	ctx := withShutdown(context.Background())

	eventChan := generateEvents(ctx, 10*time.Millisecond, 1000, 50)
	setChan := groupWithin(ctx, eventChan, time.Millisecond*500, 100)

	for set := range setChan {
		fmt.Printf("%s set size: %d\n", time.Now().Format(timeFmt), set.len())
	}
}

func generateEvents(ctx context.Context, interval time.Duration, count, keyRange int) <-chan event {
	out := make(chan event)

	go func() {
		defer close(out)

		ticker := time.NewTicker(interval)
		for i := count; i > 0; i-- {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				out <- event{
					entityID: fmt.Sprintf("event_%d", rand.Intn(keyRange)),
				}
			}
		}
	}()

	return out
}

func groupWithin(ctx context.Context, inChan <-chan event, window time.Duration, maxBatchSize int) <-chan eventSet {
	out := make(chan eventSet)

	go func() {
		defer close(out)

		set := make(eventSet)
		timer := time.NewTimer(window)

		reset := func() {
			set = make(eventSet)
			timer.Reset(window)
		}

		for {
			select {
			case <-ctx.Done(): // signal to cancel
				return
			case <-timer.C: // timer fired
				if set.len() == 0 {
					timer.Reset(window)
					continue
				}
				out <- set
				reset()
			case ev, ok := <-inChan: // event received
				if !ok { // channel closed
					return
				}
				set.put(ev)
				if set.len() < maxBatchSize {
					continue
				}
				out <- set
				reset()
			}
		}
	}()

	return out
}

func withShutdown(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fmt.Printf("%s os interrupt received\n", time.Now().Format(timeFmt))
		cancel()
	}()
	return ctx
}

type event struct {
	entityID string
}

type eventSet map[string]event

func (e eventSet) put(ev event) {
	e[ev.entityID] = ev
}

func (e eventSet) len() int {
	return len(e)
}
