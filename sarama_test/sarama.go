package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"strings"
)

var (
	brokers  = "centos08:9092"
	version  = "2.7.0"
	group    = ""
	topics   = ""
	assignor = ""
	oldest   = true
	verbose  = false
)

func main() {
	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	config := sarama.NewConfig()
	config.Version = version
	admin, err := sarama.NewClusterAdmin(strings.Split(brokers, ","), config)
	if err != nil {
		log.Panicln(err)
	}
	mapGroups,_:=admin.ListConsumerGroups()
	if err != nil {
		log.Panicln(err)
	}
	fmt.Println(dump(mapGroups))
	topics,err:=admin.ListTopics()
	if err != nil {
		log.Panicln(err)
	}

	for k,v := range topics {
		fmt.Println(k,v.NumPartitions)
	}

	var groups []string
	for c,_ := range mapGroups {
		groups=append(groups, c)
	}

	off,err:=admin.ListConsumerGroupOffsets("myTest",map[string][]int32{"test": []int32{0}})
	if err != nil {
		log.Panicln(err)
	}
	fmt.Println("Err: ",off.Err)
	fmt.Println("Offset: ",off.GetBlock("test",0).Offset)
}


func dump(data interface{}) string {
	b,_:=json.MarshalIndent(data, "", "  ")
	return string(b)
}