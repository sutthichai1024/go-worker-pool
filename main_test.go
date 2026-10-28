package main

import (
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Test A — processJob()
// ---------------------------------------------------------------------------

func TestProcessJob_Success(t *testing.T) {
	id := 3
	value, err := processJob(id)
	if err != nil {
		t.Fatalf("expected nil error for job %d, got %v", id, err)
	}
	if value != id*2 {
		t.Fatalf("expected value %d, got %d", id*2, value)
	}
}

func TestProcessJob_Job5Fails(t *testing.T) {
	value, err := processJob(5)
	if err == nil {
		t.Fatal("expected an error for job 5, got nil")
	}
	if err.Error() != "Job 5 failed" {
		t.Fatalf("unexpected error message: %v", err)
	}
	// Failed jobs still return a zero value; that is not a successful outcome.
	if value != 0 {
		t.Fatalf("expected zero value on failure, got %d", value)
	}
}

// ---------------------------------------------------------------------------
// Test B — Worker Pool and Results (uses real jobsChan)
// ---------------------------------------------------------------------------

func TestWorkerPool_CollectsAllResults(t *testing.T) {
	const numWorkers = 3
	const numJobs = 10

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	jobs := make(chan int, numJobs)
	results := make(chan JobResult)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			jobsChan(workerID, jobs, results)
		}(w)
	}

	for i := 1; i <= numJobs; i++ {
		jobs <- i
	}
	close(jobs)

	// Close results only after every worker exits — same lifecycle as main().
	go func() {
		wg.Wait()
		close(results)
	}()

	collected := make([]JobResult, 0, numJobs)
	done := make(chan struct{})
	go func() {
		for r := range results {
			collected = append(collected, r)
		}
		close(done)
	}()

	// ~10 jobs × 1s sleep / 3 workers ≈ 4s; allow headroom without flaky ms asserts.
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for results — possible deadlock or hang")
	}

	if len(collected) != numJobs {
		t.Fatalf("expected %d results, got %d", numJobs, len(collected))
	}

	seen := make(map[int]int)
	success, failed := 0, 0
	for _, r := range collected {
		seen[r.JobID]++
		if r.JobID == 5 {
			if r.Err == nil {
				t.Error("job 5 should report an error")
			} else if r.Err.Error() != "Job 5 failed" {
				t.Errorf("job 5 unexpected error: %v", r.Err)
			}
			failed++
			continue
		}
		if r.Err != nil {
			t.Errorf("job %d unexpectedly failed: %v", r.JobID, r.Err)
			failed++
			continue
		}
		if r.Value != r.JobID*2 {
			t.Errorf("job %d: expected value %d, got %d", r.JobID, r.JobID*2, r.Value)
		}
		success++
	}

	for id := 1; id <= numJobs; id++ {
		if seen[id] != 1 {
			t.Errorf("job ID %d appeared %d times; want exactly once", id, seen[id])
		}
	}

	if success != 9 {
		t.Errorf("expected 9 successes, got %d", success)
	}
	if failed != 1 {
		t.Errorf("expected 1 failure, got %d", failed)
	}
}

// ---------------------------------------------------------------------------
// Test C — Native Go channel semantics (not application logic)
// ---------------------------------------------------------------------------

func TestChannel_UnbufferedSync(t *testing.T) {
	ch := make(chan int)

	sent := make(chan struct{})
	go func() {
		ch <- 42 // blocks until a receiver is ready
		close(sent)
	}()

	select {
	case <-sent:
		t.Fatal("unbuffered send completed before receive — should have blocked")
	case <-time.After(50 * time.Millisecond):
		// Expected: sender still waiting.
	}

	v := <-ch
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("sender did not unblock after receive")
	}
}

func TestChannel_BufferedCapacity(t *testing.T) {
	ch := make(chan int, 2)

	ch <- 1
	ch <- 2

	blocked := make(chan struct{})
	go func() {
		ch <- 3 // buffer full → blocks until space frees
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("send into full buffer should block")
	case <-time.After(50 * time.Millisecond):
	}

	<-ch // free one slot
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("send should proceed after buffer has space")
	}
}

func TestChannel_ReceiveAfterClose(t *testing.T) {
	ch := make(chan int, 2)
	ch <- 10
	ch <- 20
	close(ch)

	got := make([]int, 0, 2)
	for v := range ch {
		got = append(got, v)
	}

	if len(got) != 2 || got[0] != 10 || got[1] != 20 {
		t.Fatalf("expected [10 20] after close, got %v", got)
	}
}
