package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	WorkerID int
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}


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

	/* init worker */
	worker := worker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.register()
	worker.run()
}

func (w *worker) run() {
	for {
		task := w.getTask()
		if task == nil {
			return // exit when no task avaliable
		}
		w.doTask(task)
	}
}

func (w *worker) doTask(task *Task) {
	switch task.Phase {
	case MapPhase:
		w.doMap(task)
	case ReducePhase:
		w.doReduce(task)
	default:
		panic("invalid worker task phase")
	}
}

func (w *worker) doMap(task *Task) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Println(err)
		w.reportTask(task, false)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Println(err)
		w.reportTask(task, false)
		return
	}
	file.Close()

	kvs := w.mapf(task.FileName, string(content))
	reduces := make([][]KeyValue, task.NReduce)

	// map
	for _, kv := range kvs {
		index := ihash(kv.Key) % task.NReduce
		reduces[index] = append(reduces[index], kv)
	}

	// store to intermediate file
	for i, reduce := range reduces {
		fileName := GetMapName(task.Seq, i)
		file, err := os.Create(fileName)
		if err != nil {
			log.Println(err)
			w.reportTask(task, false)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range reduce {
			err := enc.Encode(&kv)
			if err != nil {
				log.Println(err)
				w.reportTask(task, false)
				return
			}
		}
		file.Close()
	}

	w.reportTask(task, true)
}

func (w *worker) doReduce(task *Task) {
	// read all assigned intermediate file and organize them by key
	maps := make(map[string][]string)
	for i := 0; i < task.MMap; i++ {
		fileName := GetMapName(i, task.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			log.Println(err)
			w.reportTask(task, false)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	// reduce
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	resultFileName := GetReduceName(task.Seq)
	if err := ioutil.WriteFile(resultFileName, []byte(strings.Join(res, "")), 0600); err != nil {
		if err != nil {
			log.Println(err)
			w.reportTask(task, false)
			return
		}
	}

	w.reportTask(task, true)
}

/* RPC call */

func (w *worker) register() {
	args := &RegArgs{}
	reply := &RegReply{}
	call("Master.RegisterWorker", args, reply)
	w.WorkerID = reply.WorkerID
}

func (w *worker) getTask() *Task {
	args := &TaskArgs{}
	reply := &TaskReply{}
	args.WorkerID = w.WorkerID
	call("Master.GetOneTask", args, reply)

	return reply.Task
}

func (w *worker) reportTask(task *Task, isFinished bool) {
	args := &ReportArgs{}
	reply := &ReportReply{}
	args.IsFinished = isFinished
	args.Seq = task.Seq
	args.WorkerID = w.WorkerID
	args.TaskPhase = task.Phase
	call("Master.ReportTask", args, reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
