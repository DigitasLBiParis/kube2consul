package concurrent

import (
	"runtime"
	"sync"

	"github.com/golang/glog"
)

// JobFunc the function type to be run by the jobs
type JobFunc func(int, interface{})

// Action data used by the jobs
// * Name the type of action, should match a name of a function in the jobFunctions map
// will used to select the function to run
// Data the data to pass to the jobFunc
type Action struct {
	Name string
	Data interface{}
}

func threadMain(id int, queue chan Action, wg *sync.WaitGroup, jobs map[string]JobFunc) (playCommand chan bool, pauseCommand chan bool, quitCommand chan bool) {
	quitCommand = make(chan bool, 1)
	pauseCommand = make(chan bool, 1)
	playCommand = make(chan bool, 1)
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
			case <-pauseCommand:
				select {
				case <-playCommand:
				case <-quitCommand:
					return
				}
			case <-playCommand:
			}
		}
	}()
	return
}

// RunWorkers create and run jobs
// * queue channel of Action type, jobs will listen to this queue
// * jobFunctions containing the jobFunc to be used by the jobs and the action nmaes used to select
// one of these functions
// * workersNumber the number of jobs
//   - if workersNumber <= 0 or workersNumber > 2*cpuCount ==> runtime.NumCPU() will be used
//   - otherwise workersNumber will be used
func RunWorkers(queue chan Action, jobFunctions map[string]JobFunc, workersNumber int) (play func(), pause func(), quit func()) {
	var wg sync.WaitGroup
	cpuCount := runtime.NumCPU()
	jobCount := cpuCount
	if workersNumber > 0 && workersNumber <= 2*cpuCount {
		jobCount = workersNumber
	}
	runtime.GOMAXPROCS(cpuCount)

	glog.Infof("Running %d jobs", jobCount)

	playCommands := make([]chan bool, jobCount)
	pauseCommands := make([]chan bool, jobCount)
	quitCommands := make([]chan bool, jobCount)
	for i := 0; i < jobCount; i++ {
		playCommands[i], pauseCommands[i], quitCommands[i] = threadMain(i+1, queue, &wg, jobFunctions)
	}
	pause = func() {
		for _, pauseCommand := range pauseCommands {
			pauseCommand <- true
		}
		wg.Wait()
	}
	play = func() {
		for _, playCommand := range playCommands {
			playCommand <- true
		}
	}
	quit = func() {
		for _, quitCommand := range quitCommands {
			quitCommand <- true
		}
		wg.Wait()
	}
	return
}
