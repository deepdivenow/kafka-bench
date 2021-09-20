package config

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"sync"
)

type RunJobType int

const (
	Read RunJobType = iota + 1
	Write
)

type taskArgs struct {
	JobType      RunJobType
	JobName      string
	JobPartition string
	BackupType   string
	Debug        bool
	DBNow        string
	TableNow     string
}

type config struct {
	BrokerRead  kafka.ConfigMap `yaml:"broker_read"`
	BrokerWrite kafka.ConfigMap `yaml:"broker_write"`
	TaskArgs    taskArgs        `yaml:"-"`
}

var (
	once     sync.Once
	instance *config
)

func New() *config {
	once.Do(func() {
		instance = new(config)
	})
	return instance
}

func (c *config) Read(filename string) error {
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
		return err
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Printf("Unmarshal: %v", err)
		return err
	}
	return nil
}
