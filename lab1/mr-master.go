package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Jstate int

const (
	IDLE Jstate = iota
	RUNNING
	DONE
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type JobMetaInfo struct {
	StartTime time.Time
	Condition Jstate // can be running, finished, idle
	Job       Reply
}
type Master struct {
	// Your definitions here.
	nReduce      int
	mu           sync.Mutex
	MapJobs      []JobMetaInfo
	ReduceJobs   []JobMetaInfo
	Phase        Phase
	MapDone      int
	MapUnDone    int
	ReduceDone   int
	ReduceUnDone int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) DistributeTask(args *Args, reply *Reply) error {
	m.mu.Lock()
	// change if to for, range to see any job that is idle or running over some time
	if m.Phase == MapPhase {
		Distributed := false
		for i := range m.MapJobs {
			if m.MapJobs[i].Condition == IDLE {
				m.MapJobs[i].Condition = RUNNING
				m.MapJobs[i].StartTime = time.Now()
				*reply = m.MapJobs[i].Job
				m.MapUnDone++
				Distributed = true
				//fmt.Println("Job condition is", job.Condition, "(0: IDLE, 1: RUNNING, 2: DONE)")
				fmt.Printf("Distributing map task %v\n", reply.Jid)
				fmt.Printf("%v map jobs distributed, %v map jobs done, %v map jobs in total\n", m.MapUnDone, m.MapDone, len(m.MapJobs))
				break
			} else if m.MapJobs[i].Condition == RUNNING && time.Since(m.MapJobs[i].StartTime).Seconds() > 10 {
				m.MapJobs[i].StartTime = time.Now()
				*reply = m.MapJobs[i].Job
				Distributed = true
				fmt.Println("Redistributing dead map task", reply.Jid)
				break
			}
		}
		if !Distributed {
			reply.Jtype = WAIT
		}
	} else if m.Phase == ReducePhase {
		Distributed := false
		for i := range m.ReduceJobs {
			if m.ReduceJobs[i].Condition == IDLE {
				m.ReduceJobs[i].Condition = RUNNING
				m.ReduceJobs[i].StartTime = time.Now()
				*reply = m.ReduceJobs[i].Job
				m.ReduceUnDone++
				Distributed = true
				//fmt.Println("Job condition is", job.Condition, "(0: IDLE, 1: RUNNING, 2: DONE)")
				fmt.Printf("Distributing reduce task %v\n", reply.Jid)
				fmt.Printf("%v reduce jobs distributed, %v reduce jobs done, %v reduce jobs in total\n", m.ReduceUnDone, m.ReduceDone, len(m.ReduceJobs))
				break
			} else if m.ReduceJobs[i].Condition == RUNNING && time.Since(m.ReduceJobs[i].StartTime).Seconds() > 10 {
				m.ReduceJobs[i].StartTime = time.Now()
				*reply = m.ReduceJobs[i].Job
				Distributed = true
				fmt.Println("Redistributing dead reduce task", reply.Jid)
				break
			}
		}
		if !Distributed {
			reply.Jtype = WAIT
		}
	} else {
		reply.Jtype = EXIT
	}
	m.mu.Unlock()
	return nil
}

func (m *Master) TaskDone(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.Jtype {
	case MAP:
		if m.MapJobs[args.Jid].Condition == RUNNING {
			m.MapJobs[args.Jid].Condition = DONE
			m.MapDone++
			fmt.Printf("Map task %v done\n", args.Jid)
		} else if m.MapJobs[args.Jid].Condition == DONE {
			fmt.Printf("Job %v done by other worker\n", args.Jid)
		} else if m.MapJobs[args.Jid].Condition == IDLE {
			fmt.Printf("Job %v not started\n", args.Jid)
		}
		fmt.Printf("%v map jobs distributed, %v map jobs done, %v map jobs in total\n", m.MapUnDone, m.MapDone, len(m.MapJobs))
		if (m.MapDone == m.MapUnDone) && (m.MapDone == len(m.MapJobs)) {
			m.NextPhase()
		}
	case REDUCE:
		if m.ReduceJobs[args.Jid].Condition == RUNNING {
			m.ReduceJobs[args.Jid].Condition = DONE
			m.ReduceDone++
			fmt.Printf("Reduce task %v done\n", args.Jid)
		} else if m.ReduceJobs[args.Jid].Condition == DONE {
			fmt.Printf("Job %v done by other worker\n", args.Jid)
		} else if m.ReduceJobs[args.Jid].Condition == IDLE {
			fmt.Printf("Job %v not started\n", args.Jid)
		}
		fmt.Printf("%v Reduce jobs distributed, %v Reduce jobs done, %v Reduce jobs in total\n", m.ReduceUnDone, m.ReduceDone, len(m.ReduceJobs))
		if (m.ReduceDone == m.ReduceUnDone) && (m.ReduceDone == len(m.ReduceJobs)) {
			m.NextPhase()
		}
	}
	return nil
}

func (m *Master) NextPhase() {
	if m.Phase == MapPhase {
		m.MakeReduceTask()
		fmt.Printf("ALL map tasks done, entering next phase.\n\n")
		m.Phase = ReducePhase
	} else if m.Phase == ReducePhase {
		fmt.Printf("ALL reduce tasks done, entering next phase.\n\n")
		m.Phase = AllDone
	} else {
		fmt.Printf("ALL done.\n")
	}
}

func (m *Master) MakeReduceTask() {
	// make the reduce tasks waiting to be distributed to workers
	for i := 0; i < m.nReduce; i++ {
		job := JobMetaInfo{
			StartTime: time.Now(),
			Condition: IDLE,
			Job:       Reply{m.nReduce, REDUCE, m.SelectReduceFiles(i), i},
		}
		m.ReduceJobs = append(m.ReduceJobs, job)
		//fmt.Println("Made reduce task ", i)
	}
}
func (m *Master) SelectReduceFiles(i int) []string {
	strs := []string{}
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
			strs = append(strs, file.Name())
		}
	}
	return strs
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Phase == AllDone {
		fmt.Println("All tasks done, master terminated!")
		return true
	} else {
		return false
	}
}

func (m *Master) MakeMapTask(files []string) {
	// make the map tasks waiting to be distributed to workers
	for i, file := range files {
		job := JobMetaInfo{
			StartTime: time.Now(),
			Condition: IDLE,
			Job:       Reply{m.nReduce, MAP, []string{file}, i},
		}
		m.MapJobs = append(m.MapJobs, job)
		// fmt.Println("Made map task ", i)
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce, sync.Mutex{}, make([]JobMetaInfo, 0), make([]JobMetaInfo, 0), MapPhase, 0, 0, 0, 0}
	// Your code here.
	m.MakeMapTask(files)
	m.server()
	return &m
}
