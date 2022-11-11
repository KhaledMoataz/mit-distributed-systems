package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallAskForTask()

		if reply.TaskType == 1 {
			fmt.Printf("Map Task - FileName: %v\n", reply.FileName)
			executeMapTask(reply.FileName, reply.Id, reply.NumReduce, mapf)
		} else if reply.TaskType == 2 {
			fmt.Printf("Reduce Task - Locations %v\n", reply.FilesLocation)
			executeReduceTask(reply.FilesLocation, reply.Id, reducef)
		} else {
			fmt.Printf("No Free Task!\n")
			time.Sleep(time.Second)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func executeMapTask(fileName string, fileId int, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kva := mapf(fileName, string(content))

	// TODO: Write intermediate files in file
	files := make([]os.File, nReduce)
	fileEncoders := make([]json.Encoder, nReduce)
	fileNames := make([]string, nReduce)

	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%v-%v", fileId, i)
		ofile, _ := os.Create(oname)
		files[i] = *ofile
		fileNames[i] = oname
		fileEncoders[i] = *json.NewEncoder(ofile)
	}

	for _, kv := range kva {
		enc := fileEncoders[ihash(kv.Key)%nReduce]
		err := enc.Encode(&kv)

		if err != nil {
			fmt.Printf("Encoding Error!\n")
		}
	}

	for i := 0; i < nReduce; i++ {
		files[i].Close()
	}

	CallMapCompletion(fileName, fileNames)
}

func executeReduceTask(fileLocations []string, bucketId int, reducef func(string, []string) string) {
	// TODO: Implement Logic
	var kva []KeyValue
	for _, fileName := range fileLocations {

		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", bucketId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	CallReduceCompletion(bucketId)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallAskForTask() AskForTaskReply {
	args := AskForTaskArgs{WorkerId: os.Getpid()}

	reply := AskForTaskReply{}

	ok := call("Coordinator.AskForTask", &args, &reply)

	if !ok {
		fmt.Printf("AskForTask call Failed!\n")
	}

	return reply
}

func CallMapCompletion(fileName string, filesLocation []string) {
	args := MapCompletionArgs{FileName: fileName, FilesLocation: filesLocation}

	reply := MapCompletionReply{}

	ok := call("Coordinator.MapCompletion", &args, &reply)

	if !ok {
		fmt.Printf("MapCompletion call Failed!\n")
	}
}

func CallReduceCompletion(index int) {
	args := ReduceCompletionArgs{index}

	reply := ReduceCompletionReply{}

	ok := call("Coordinator.ReduceCompletion", &args, &reply)

	if !ok {
		fmt.Printf("ReduceCompletion call Failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
