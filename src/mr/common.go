package mr

import "fmt"

type TaskPhase int

const (
	MapPhase TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	NReduce int
	MMap int
	Seq int
	Phase TaskPhase // reduce task or map task
}

func GetMapName(taskSeq int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", taskSeq, reduceIndex)
}

func GetReduceName(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}