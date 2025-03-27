// Requires cfgBytes.txt to contain output of GET /api/cfg
// Run as: go run parseCfg.go

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"

	"github.com/couchbase/cbgt"
)

const vbuckets = 1024

type X struct {
	IndexDefs     *cbgt.IndexDefs    `json:"indexDefs"`
	PlanPIndexes  *cbgt.PlanPIndexes `json:"planPIndexes"`
	NodeDefsKnown *cbgt.NodeDefs     `json:"nodeDefsKnown"`
}

func unmarshalCfgPayload() (rv *X, err error) {
	cfgBytes, err := ioutil.ReadFile("cfgBytes.txt")
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(cfgBytes, &rv)
	return rv, err
}

func main() {
	payload, err := unmarshalCfgPayload()
	if err != nil {
		fmt.Println(err)
		return
	}

	nodeDefsKnown := map[string]string{}

	if payload.NodeDefsKnown == nil {
		fmt.Println("no node defs in system")
		return
	}

	for k, v := range payload.NodeDefsKnown.NodeDefs {
		nodeDefsKnown[k] = v.HostPort
	}

	nodeActiveCount := map[string]int{}
	nodeReplicaCount := map[string]int{}

	indexActiveCount := map[string]map[string]int{}
	indexReplicaCount := map[string]map[string]int{}

	if payload.PlanPIndexes != nil {
		for _, v := range payload.PlanPIndexes.PlanPIndexes {
			if _, exists := indexActiveCount[v.IndexName]; !exists {
				indexActiveCount[v.IndexName] = make(map[string]int)
			}
			if _, exists := indexReplicaCount[v.IndexName]; !exists {
				indexReplicaCount[v.IndexName] = make(map[string]int)
			}

			for k1, v1 := range v.Nodes {
				if v1.Priority == 0 {
					nodeActiveCount[k1]++
					indexActiveCount[v.IndexName][k1]++
				} else {
					nodeReplicaCount[k1]++
					indexReplicaCount[v.IndexName][k1]++
				}
			}
		}
	}

	var actualPartitionCount int

	fmt.Println("Actives:")
	if len(nodeActiveCount) == 0 {
		fmt.Printf("\tN/A\n")
	}
	for k, v := range nodeActiveCount {
		fmt.Printf("\t%s : %d\n", nodeDefsKnown[k], v)
		actualPartitionCount += v
	}
	fmt.Println("Replicas:")
	if len(nodeReplicaCount) == 0 {
		fmt.Printf("\tN/A\n")
	}
	for k, v := range nodeReplicaCount {
		fmt.Printf("\t%s : %d\n", nodeDefsKnown[k], v)
		actualPartitionCount += v
	}

	fmt.Printf("Actual number of index partitions in cluster: %v\n", actualPartitionCount)

	indexesMap := map[string]string{}
	var expectedPartitionCount int

	if payload.IndexDefs != nil {
		for k, v := range payload.IndexDefs.IndexDefs {
			indexesMap[k] = fmt.Sprintf("maxPartitionsPerPIndex: %v, indexPartitions: %v, numReplicas: %v",
				v.PlanParams.MaxPartitionsPerPIndex, v.PlanParams.IndexPartitions, v.PlanParams.NumReplicas)

			var currActivePartitions int
			if v.PlanParams.IndexPartitions > 0 {
				currActivePartitions = v.PlanParams.IndexPartitions
			} else if v.PlanParams.MaxPartitionsPerPIndex > 0 {
				currActivePartitions = int(math.Ceil(float64(vbuckets) / float64(v.PlanParams.MaxPartitionsPerPIndex)))
			}
			currReplicaPartitions := currActivePartitions * v.PlanParams.NumReplicas
			expectedPartitionCount += currActivePartitions + currReplicaPartitions
		}
	}

	fmt.Printf("Expected number of index partitions in cluster: %v\n", expectedPartitionCount)
	fmt.Printf("Indexes: %d\n", len(indexesMap))
	for k, v := range indexesMap {
		fmt.Printf("\t%s :: %s\n", k, v)
	}
	fmt.Println()

	fmt.Printf("Index actives distribution:\n")
	for k, v := range indexActiveCount {
		fmt.Printf("\tIndex: %v\n", k)
		for k1, v1 := range v {
			fmt.Printf("\t\t%s : %d\n", nodeDefsKnown[k1], v1)
		}
	}

	fmt.Printf("Index replicas distribution:\n")
	for k, v := range indexReplicaCount {
		fmt.Printf("\tIndex: %v\n", k)
		for k1, v1 := range v {
			fmt.Printf("\t\t%s : %d\n", nodeDefsKnown[k1], v1)
		}
	}
	fmt.Println()
}
