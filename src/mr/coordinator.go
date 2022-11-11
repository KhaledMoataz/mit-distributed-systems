package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const WORKER_TIMEOUT = 10 // seconds

type FileState struct {
	processingState int64 // -1: not processed, 0: processed, 1-WORKER_TIMEOUT: current processing time
	wokerId         int   // -1 in case of not processed
}
type InputFile struct {
	name  string
	state FileState
}

type IntermediateFile struct {
	names []string
	state FileState
}
type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	inputFile         []InputFile
	intermediateFiles []IntermediateFile
	nReduce           int
	isMapDone         bool
	isReduceDone      bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func getCurrentTime() int64 {
	return time.Now().Unix()
}

func refreshFileState(file *FileState) {
	if file.processingState < 1 {
		return
	}

	timeDiff := getCurrentTime() - file.processingState

	if timeDiff > WORKER_TIMEOUT {
		// TODO: Kill worker id and erase all its files
		file.processingState = -1
		file.wokerId = -1
	}
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	fmt.Printf("Worker %v is asking for a task!\n", args.WorkerId)

	c.mu.Lock()
	defer c.mu.Unlock()
	reply.TaskType = 0

	if !c.isMapDone {
		for fileIdx := 0; fileIdx < len(c.inputFile); fileIdx++ {
			refreshFileState(&c.inputFile[fileIdx].state)

			if c.inputFile[fileIdx].state.processingState == -1 {
				c.inputFile[fileIdx].state.processingState = getCurrentTime()
				c.inputFile[fileIdx].state.wokerId = args.WorkerId

				reply.TaskType = 1
				reply.FileName = c.inputFile[fileIdx].name
				reply.Id = fileIdx
				reply.NumReduce = c.nReduce
				break
			}
		}

		return nil
	}

	if !c.isReduceDone {
		for fileIdx := 0; fileIdx < len(c.intermediateFiles); fileIdx++ {
			refreshFileState(&c.intermediateFiles[fileIdx].state)

			if c.intermediateFiles[fileIdx].state.processingState == -1 {
				c.intermediateFiles[fileIdx].state.processingState = getCurrentTime()
				c.intermediateFiles[fileIdx].state.wokerId = args.WorkerId

				reply.TaskType = 2
				reply.FilesLocation = c.intermediateFiles[fileIdx].names
				reply.Id = fileIdx
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) MapCompletion(args *MapCompletionArgs, reply *MapCompletionReply) error {
	fmt.Printf("Map Task Completed: %v\n", args.FileName)

	c.mu.Lock()
	defer c.mu.Unlock()
	foundFile := false
	c.isMapDone = true

	for fileIdx := 0; fileIdx < len(c.inputFile); fileIdx++ {
		if c.inputFile[fileIdx].name == args.FileName {
			c.inputFile[fileIdx].state.processingState = 0
			foundFile = true
		}

		if c.inputFile[fileIdx].state.processingState != 0 {
			c.isMapDone = false
		}
	}

	if foundFile {
		for i, v := range args.FilesLocation {
			c.intermediateFiles[i].names = append(c.intermediateFiles[i].names, v)
			// fmt.Printf("Intermediate file %v size: %v\n", i, len(c.intermediateFiles[i].names))
		}
		return nil
	}

	return fmt.Errorf("not found: %s", args.FileName)
}

func (c *Coordinator) ReduceCompletion(args *ReduceCompletionArgs, reply *ReduceCompletionReply) error {
	fmt.Printf("Reduce Task Completed: %v\n", args.Index)

	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Index < c.nReduce {
		c.intermediateFiles[args.Index].state.processingState = 0
	} else {
		return fmt.Errorf("not found reduce bucket %v", args.Index)
	}

	c.isReduceDone = true

	for i := 0; i < len(c.intermediateFiles) && c.isReduceDone; i++ {
		if c.intermediateFiles[i].state.processingState != 0 {
			c.isReduceDone = false
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.isMapDone && c.isReduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce, intermediateFiles: make([]IntermediateFile, nReduce)}

	// Your code here.
	for _, fileName := range files {
		c.inputFile = append(c.inputFile, InputFile{name: fileName, state: FileState{-1, -1}})
	}

	for i := 0; i < nReduce; i++ {
		c.intermediateFiles[i].state.processingState = -1
	}

	c.server()
	return &c
}
