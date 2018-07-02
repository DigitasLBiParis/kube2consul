package concurrent

import (
	"runtime"
	"sync"
)

type JobFunc func(int, interface{})

type Action struct {
	Name string
	Data interface{}
}

func threadMain(id int, queue chan Action, wg *sync.WaitGroup, jobs map[string]JobFunc) chan bool {
	quitCommand := make(chan bool, 1)
	go func() {
		for {
			select {
			case action := <-queue:
				wg.Add(1)
				if job, ok := jobs[action.Name]; ok {
					job(id, action.Data)
				}
				wg.Done()
			case <-quitCommand:
				return
			}

		}
	}()
	return quitCommand
}

func RunWorkers(queue chan Action, jobs map[string]JobFunc) func() {
	var wg sync.WaitGroup
	cpuCount := runtime.NumCPU()
	runtime.GOMAXPROCS(cpuCount)

	quitCommands := make([]chan bool, cpuCount)
	for i := 0; i < cpuCount; i++ {
		quitCommands[i] = threadMain(i+1, queue, &wg, jobs)
	}
	return func() {
		for _, quitCommand := range quitCommands {
			quitCommand <- true
		}
		wg.Wait()
	}
}
