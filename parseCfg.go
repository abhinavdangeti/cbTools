// Requires cfgBytes.txt to contain output of GET /api/cfg
// Run as: go run parseCfg.go

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/couchbase/cbgt"
)

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

	for k, v := range payload.NodeDefsKnown.NodeDefs {
		nodeDefsKnown[k] = v.HostPort
	}

	nodeActiveCount := map[string]int{}
	nodeReplicaCount := map[string]int{}

	for _, v := range payload.PlanPIndexes.PlanPIndexes {
		for k1, v1 := range v.Nodes {
			if v1.Priority == 0 {
				nodeActiveCount[k1]++
			} else {
				nodeReplicaCount[k1]++
			}
		}
	}

	fmt.Println("Actives:")
	for k, v := range nodeActiveCount {
		fmt.Printf("\t%s : %d\n", nodeDefsKnown[k], v)
	}
	fmt.Println("Replicas:")
	for k, v := range nodeReplicaCount {
		fmt.Printf("\t%s : %d\n", nodeDefsKnown[k], v)
	}

	indexesMap := map[string]string{}

	if payload.IndexDefs != nil {
		for k, v := range payload.IndexDefs.IndexDefs {
			indexesMap[k] = fmt.Sprintf("maxPartitionsPerPIndex: %v, indexPartitions: %v, numReplicas: %v",
				v.PlanParams.MaxPartitionsPerPIndex, v.PlanParams.IndexPartitions, v.PlanParams.NumReplicas)
		}
	}

	fmt.Printf("Indexes: %d\n", len(indexesMap))
	for k, v := range indexesMap {
		fmt.Printf("\t%s :: %s\n", k, v)
	}
	fmt.Println()
}
