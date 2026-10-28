package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	green  = "\033[32m"
	blue   = "\033[34m"
	yellow = "\033[33m"
	reset  = "\033[0m"
)

type JobResult struct {
    JobID int
    Value int
    Err   error
}

func processJob(id int) (int, error) {
	
    fmt.Printf("Start Job %d\n", id)

    time.Sleep(1 * time.Second)
	if id == 5 {
		return  0, errors.New("Job 5 failed")
	}

    fmt.Printf("Done Job %d\n", id)
	return  id*2,nil
}

func jobsChan(workerID int, jobs chan int, results chan JobResult) {
    round := 0

    for job := range jobs {
        round++

	

        result,err := processJob(job)

		results <- JobResult{
			JobID: job,
			Value: result,
			Err:   err,
		}

		fmt.Printf(
			"%s[Worker %d] Round %d -> Job %d%s -> %d\n",
			blue, workerID, round, job, reset, result,
		)
    }
}

func main() {
	var wg sync.WaitGroup

	wg.Add(3)
	jobs := make(chan int, 10)
	results := make(chan JobResult)

	
	for j := 0; j < 3; j++ {
		go func(workerID int) {
			fmt.Printf("%s[Worker %d] START%s\n", green, workerID, reset)
		
			defer wg.Done()
		
			jobsChan(workerID, jobs,results)
		
			fmt.Printf("%s[Worker %d] EXIT%s\n", yellow, workerID, reset)
		}(j)
		
	}
	
	for i := 1; i <= 10; i++ {
		start := time.Now()

		fmt.Printf("Trying Job %d\n", i)
		jobs <- i
		fmt.Printf("Sent Job %d (waited %v)\n", i, time.Since(start))
	}

	close(jobs)
	go func() {
		wg.Wait()
		close(results)
	}()
	success, failed := 0, 0
	for result := range results {
		if result.Err != nil {
			fmt.Println("[main] Failed:", result.Err)
			failed++
			continue
		}
		fmt.Println("[main] Success Job ID:", result.JobID, "Result:", result.Value)
		success++
	}

	fmt.Println("========== JOB SUMMARY ==========")
	fmt.Printf("Total Jobs : %d\n", success+failed)
	fmt.Printf("Success    : %d\n", success)
	fmt.Printf("Failed     : %d\n", failed)
	fmt.Println("=================================")
}