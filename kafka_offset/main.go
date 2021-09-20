package main

import (
	"encoding/json"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"unsafe"
)

func main() {

	kaConf := kafka.ConfigMap{"bootstrap.servers": "centos08:9092"}
	t, err := GetTopics(kaConf)
	if err != nil {
		log.Fatal(err)
	}
	info:=MakeInfo()
	for _, g := range []string{"console", "myTest"} {
		off, err := GetCommitedOffsets(kaConf, g, t)
		if err != nil {
			log.Fatal(err)
		}
		if off == nil {
			continue
		}
		info.Offsets[g] = off

	}
	fmt.Println(StringDump(info))
}

func GetTopics(brokerConfig kafka.ConfigMap) ([]kafka.TopicPartition, error) {
	var res []kafka.TopicPartition
	a, err := kafka.NewAdminClient(&brokerConfig)
	if err != nil {
		return res, err
	}
	defer a.Close()
	t, err := a.GetMetadata(nil, true, 15*1000)
	if err != nil {
		return res, err
	}
	for _, v := range t.Topics {
		newTopic := StringClone(v.Topic)
		for i := 0; i < len(v.Partitions); i++ {
			res = append(res, kafka.TopicPartition{Topic: &newTopic, Partition: int32(i)})
		}
	}
	return res, nil
}

func MakeInfo() Info{
	var info Info
	info.Offsets=map[string][]GroupOffsets{}
	return info
}

type Info struct {
	Offsets map[string][]GroupOffsets `json:"offsets"`
}

type GroupOffsets struct {
	GroupName       string `json:"group_name"`
	TopicName       string `json:"topic_name"`
	TopicPartition  int32  `json:"topic_part"`
	Min             int64  `json:"min"`
	Max             int64  `json:"max"`
	CommittedOffset int64  `json:"current"`
}

func GetCommitedOffsets(brokerConfig kafka.ConfigMap, group string, topics []kafka.TopicPartition) ([]GroupOffsets, error) {
	var res []GroupOffsets
	brokerConfig["group.id"] = group
	c, err := kafka.NewConsumer(&brokerConfig)
	if err != nil {
		return res, err
	}
	defer c.Close()
	committedOffsets, err := c.Committed(topics, 5000)
	if err != nil {
		return res, err
	}
	for _, co := range committedOffsets {

		if co.Offset >= 0 {
			min, max, err := c.QueryWatermarkOffsets(*co.Topic, co.Partition, 15*1000)
			if err != nil {
				return res, err
			}
			gO := GroupOffsets{
				GroupName:       group,
				TopicName:       *co.Topic,
				TopicPartition:  co.Partition,
				Min:             min,
				Max:             max,
				CommittedOffset: int64(co.Offset),
			}
			res = append(res, gO)
		}
	}
	return res, nil
}

func StringClone(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}

func StringDump(data interface{}) string {
	b, _ := json.MarshalIndent(data, "", "  ")
	return string(b)
}
