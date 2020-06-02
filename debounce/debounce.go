package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"
)

const timeFmt = "[15:04:05]"

func main() {
	ctx := withShutdown(context.Background())

	events := generateEvents(ctx, 1000)
	batches := groupWithin(ctx, events, time.Millisecond*500, 100)

	for b := range batches {
		fmt.Printf("%s batch size: %d\n", time.Now().Format(timeFmt), b.len())
	}
}

func generateEvents(ctx context.Context, count int) <-chan event {
	out := make(chan event)

	go func() {
		defer close(out)

		ticker := time.NewTicker(10 * time.Millisecond)
		for count > 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				out <- event{
					entityID: fmt.Sprintf("event_%d", count),
				}
				count--
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
