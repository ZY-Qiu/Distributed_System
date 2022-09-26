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
	"strconv"
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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	CallMaster(mapf, reducef)

}

// Used to call the master asking for a task
func CallMaster(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := Args{}
	for {
		reply := Reply{}
		if !call("Master.DistributeTask", &args, &reply) {
			return
		}
		switch reply.Jtype {
		case MAP:
			fmt.Println("Receive map task", reply.Jid)
			fmt.Println("Task file name is", reply.Files[0])
			Map(&reply, mapf)
			if !CallDone(&reply) {
				return
			}
		case WAIT:
			fmt.Printf("Worker waiting.\n")
			time.Sleep(time.Second)
		case REDUCE:
			fmt.Println("Receive reduce task", reply.Jid)
			Reduce(&reply, reducef)
			if !CallDone(&reply) {
				return
			}
		case EXIT:
			fmt.Println("All tasks done, worker terminated!")
			return
		}
	}

}

func Reduce(reply *Reply, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for _, filename := range reply.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
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
	oname := "mr-out-" + strconv.Itoa(reply.Jid)
	dir, _ := os.Getwd()
	tempfile, _ := ioutil.TempFile(dir, oname)
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
		fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	os.Rename(tempfile.Name(), oname)
	tempfile.Close()
}

func CallDone(reply *Reply) bool {
	arg := Args{
		Jtype: reply.Jtype,
		Jid:   reply.Jid,
	}
	ok := call("Master.TaskDone", &arg, reply)
	if ok {
		fmt.Printf("Task %v done\n", arg.Jid)
		return true
	} else {
		log.Fatalf("Worker call done failed")
		return false
	}
}

func Map(reply *Reply, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	for _, filename := range reply.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	// here write into different intermediate files
	//oname := "mr-out-0"
	rn := reply.Rnum
	Hashedkv := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		Hashedkv[ihash(kv.Key)%rn] = append(Hashedkv[ihash(kv.Key)%rn], kv)
	}
	//ofile, _ := os.Create(oname)
	for i := 0; i < rn; i++ {
		oname := "mr-" + strconv.Itoa(reply.Jid) + "-" + strconv.Itoa(i)
		dir, _ := os.Getwd()
		tempfile, _ := ioutil.TempFile(dir, oname)
		enc := json.NewEncoder(tempfile)
		for _, kv := range Hashedkv[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("cannot encode to %v", oname)
			}
		}
		os.Rename(tempfile.Name(), oname)
		tempfile.Close()
	}

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
