package mapreduce

import (
	"encoding/json"
	//"fmt"
	"log"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	KeyValues := make(map[string][]string)

	//fmt.Println("Now get ", nMap, " rFiles")
	for i := 0; i < nMap; i++ { //循环读取每个中间文件中的键值对到KeyValues中，先不考虑内存是否足够的问题
		rFileName := reduceName(jobName, i, reduceTaskNumber)
		//rFileName := "mrtmp.test-0-0"
		rFile, err := os.Open(rFileName)
		if err != nil {
			log.Fatal("open the intermediate file: ", rFileName, "failed error: ", err)
		}

		defer rFile.Close()
		dec := json.NewDecoder(rFile)
		var kv KeyValue

		for { //从一个中间文件中循环读取出键值对，并将键值对存储到KeyValus中
			err := dec.Decode(&kv)
			//fmt.Println(kv)
			if err != nil {
				//fmt.Println("Error: ", err)
				break
			}
			//fmt.Println("reduce get a kv from json: ", kv, "the rFileName: ", rFileName)
			_, exis := KeyValues[kv.Key]
			if !exis { //没有这个key值则需要创建一个[]string的切片
				KeyValues[kv.Key] = make([]string, 0)
			}
			KeyValues[kv.Key] = append(KeyValues[kv.Key], kv.Value)
		}
	}
	//fmt.Println("KV MAP has: ", len(KeyValues))
	//keys中存放各个键值对的key，用于进行排序
	keys := make([]string, 0)
	for k, _ := range KeyValues {
		keys = append(keys, k)
	}
	//fmt.Println(len(keys))
	sort.Strings(keys)

	mFileName := mergeName(jobName, reduceTaskNumber)
	mFile, err := os.Create(mFileName)
	if err != nil {
		log.Fatal("can not ctreate ouput merge file: ", mFileName, "error: ", err)
	}
	defer mFile.Close()
	enc := json.NewEncoder(mFile)

	for _, k := range keys { //依次对各个key值调用用户提供的mapF函数并将结果写入merge文件
		var ans KeyValue
		ans.Value = reduceF(k, KeyValues[k])
		ans.Key = k
		err := enc.Encode(ans)
		if err != nil {
			log.Fatal("encode ans failed!:", ans, "error: ", err)
		}
	}
}
