package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	scheduleInterval = time.Millisecond * 500
	maxTaskRunningTime = time.Second * 10
)

const (
	TaskStatusReady = 0
	TaskStatusQueue = 1 // inside work channel, waiting to be execute
	TaskStatusRunning = 2
	TaskStatusError = 3
	TaskStatusFinish = 4
)

type TaskStat struct {
	Status int
	WorkerID int
	StartTime time.Time
}

type Master struct {
	files []string // input files
	nReduce int
	taskPhase TaskPhase // default: mapPhase
	taskStats []TaskStat
	taskChan chan Task
	workerSeq int // unique identifier for each worke
	done bool
	mux sync.Mutex // thread safe
}

/********* RPC ***********/

func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskChan
	reply.Task = &task

	m.registerTask(args.WorkerID, task.Seq, task.Phase)

	return nil
}

func (m *Master) RegisterWorker(args *RegArgs, reply *RegReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	m.workerSeq += 1
	reply.WorkerID = m.workerSeq

	return nil
}

func (m *Master) ReportTask(args *ReportArgs, reply *ReportReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	// handle task that has been re-assigned
	if args.TaskPhase != m.taskPhase || args.WorkerID != m.taskStats[args.Seq].WorkerID {
		return nil
	}

	if args.IsFinished {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusError
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) getTask(index int) (task Task) {
	task = Task{
		FileName: "",
		NReduce:  m.nReduce,
		MMap:  len(m.files),
		Seq:      index,
		Phase:    m.taskPhase,
	}
	if m.taskPhase == MapPhase {
		task.FileName = m.files[index]
	}
	return task
}

func (m *Master) registerTask(workerID int, seq int, taskPhase TaskPhase) {
	m.mux.Lock()
	defer m.mux.Unlock()

	if taskPhase != m.taskPhase {
		panic("task phase out of sync")
	}

	m.taskStats[seq].WorkerID = workerID
	m.taskStats[seq].StartTime = time.Now()
	m.taskStats[seq].Status = TaskStatusRunning
}

func (m *Master) schedule() {
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.done {
		return
	}

	isFinished := true // if we finish all existing jobs at this phase
	for i, task := range m.taskStats {
		switch task.Status {
		case TaskStatusReady:
			isFinished = false
			m.taskChan <- m.getTask(i)
			m.taskStats[i].Status = TaskStatusQueue
		case TaskStatusQueue:
			isFinished = false
		case TaskStatusRunning:
			isFinished = false
			if time.Now().Sub(m.taskStats[i].StartTime) > maxTaskRunningTime {
				m.taskStats[i].Status = TaskStatusReady
			}
		case TaskStatusError:
			isFinished = false
			m.taskStats[i].Status = TaskStatusReady
		case TaskStatusFinish:
		default:
			panic("t.status error")
		}
	}
	if isFinished {
		if m.taskPhase == MapPhase {
			m.initReduceTasks()
		} else {
			m.done = true
		}
	}
}


func (m *Master) Done() bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.done
}

// run schedule periodically
func (m *Master) tickSchedule() {
	for !m.done {
		go m.schedule()
		time.Sleep(scheduleInterval)
	}
}

func (m *Master) initMapTasks() {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
}

func (m *Master) initReduceTasks() {
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// init master
	m.mux.Lock()
	defer m.mux.Unlock()
	m.files = append(m.files, files...)
	m.nReduce = nReduce
	if (len(m.files) > nReduce) { // size of task channel is determined by max(nReduce, nMap)
		m.taskChan = make(chan Task, len(m.files))
	} else {
		m.taskChan = make(chan Task, nReduce)
	}
	m.initMapTasks()

	go m.tickSchedule()
	m.server()
	return &m
}
