package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"
	"math/rand"
	"os"
	"strconv"
)

type ComplexType struct {
	ID        uint32                 `json:"id"`
	PartnerId uint32                 `json:"partnerId"`
	Status    string                 `json:"status"`
	Caption   string                 `json:"caption"`
	Inn       map[string]interface{} `json:"inn"`
}

func main() {
	Brokers := os.Getenv("KAFKA_BROKERS")
	if len(Brokers) < 1 {
		Brokers = "127.0.0.1"
	}
	SchemaURI := os.Getenv("KAFKA_SCHEMA")
	if len(SchemaURI) < 1 {
		SchemaURI = "http://127.0.0.1:8081"
	}
	SchemaSufix := os.Getenv("KAFKA_SCHEMA_SUFIX")
	if len(SchemaSufix) < 1 {
		SchemaSufix = "value"
	}
	Topic := os.Getenv("KAFKA_TOPIC")
	if len(Topic) < 1 {
		Topic = "test"
	}
	KafkaUseAuth := false
	KafkaUseAuthEnv := os.Getenv("KAFKA_USE_AUTH")
	if len(KafkaUseAuthEnv) > 0 {
		KafkaUseAuth = true
	}
	User := os.Getenv("KAFKA_USER")
	Pass := os.Getenv("KAFKA_PASS")
	if len(User) > 1 &&  len(Pass) > 1 {
		KafkaUseAuth = true
	}
	if len(User) < 1 {
		User = "test"
	}
	if len(Pass) < 1 {
		Pass = "test"
	}
	Debug := false
	debugEnt := os.Getenv("KAFKA_DEBUG")
	if len(debugEnt) > 0 {
		Debug = true
	}

	Iterations := uint32(10000)
	if iTmp, err := strconv.Atoi(os.Getenv("KAFKA_ITERATIONS")); err == nil && iTmp > 0 {
		Iterations = uint32(iTmp)
	}

	kafkaConfig := kafka.ConfigMap{"bootstrap.servers": Brokers}

	if KafkaUseAuth {
		kafkaConfig["sasl.mechanisms"] = "PLAIN"
		kafkaConfig["security.protocol"] = "SASL_PLAINTEXT"
		kafkaConfig["sasl.username"] = User
		kafkaConfig["sasl.password"] = Pass
	}
	if Debug {
		log.Println(dump(kafkaConfig))
	}
	os.Exit(1)
	// 1) Create the producer as you would normally do using Confluent's Go client
	p, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		log.Fatalln(err)
	}
	defer p.Close()

	go func() {
		for event := range p.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				message := ev
				if ev.TopicPartition.Error != nil {
					log.Printf("Error delivering the message '%s'", message.Key)
				} else {
					if Debug {
						log.Printf("Message '%s' delivered successfully!", message.Key)
					}
				}
			}
		}
	}()

	// 2) Fetch the latest version of the schema, or create a new one if it is the first
	schemaRegistryClient := srclient.CreateSchemaRegistryClient(SchemaURI)
	schemaName := fmt.Sprintf("%s-%s", Topic, SchemaSufix)
	schema, err := schemaRegistryClient.GetLatestSchema(schemaName)
	if err != nil {
		log.Println(err)
	}
	if schema == nil {
		log.Printf("Schema not found for %s", schemaName)
		//schemaBytes, _ := ioutil.ReadFile("complexType.avsc")
		schemaString := `{"type":"record","name":"CompanyExportMessage","namespace":"su.moneycare.messages.backoffice.company.export","fields":[{"name":"id","type":"long","doc":"Unique identifier"},{"name":"partnerId","type":"long","doc":"Partner identifier"},{"name":"status","type":{"type":"enum","name":"CompanyStatus","symbols":["active","inactive"]},"doc":"Status"},{"name":"caption","type":{"type":"string","avro.java.string":"String"},"doc":"Caption"},{"name":"inn","type":[{"type":"string","avro.java.string":"String"},"null"],"doc":"Inn (10 or 12 digits)"}]}`
		schema, err = schemaRegistryClient.CreateSchema(schemaName, schemaString, srclient.Avro)
		if err != nil {
			log.Fatalf("Error creating the schema %s", err)
		}
	}
	if Debug {
		log.Printf("Schema: %s", schema.Schema())
	}
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	// 3) Serialize the record using the schema provided by the client,
	// making sure to include the schema id as part of the record.
	for i := uint32(1); i <= Iterations; i++ {
		newComplexType := ComplexType{
			ID:        i,
			PartnerId: rand.Uint32(),
			Status:    "active",
			Caption:   uuid.New().String(),
			Inn:       nil,
		}
		if Debug {
			log.Println(newComplexType)
		}
		value, err := json.Marshal(newComplexType)
		if err != nil {
			log.Fatalf("Marshal: %s", err)
		}
		native, _, err := schema.Codec().NativeFromTextual(value)
		if err != nil {
			log.Fatalf("CodecNativeFromTextual: %s", err)
		}
		valueBytes, err := schema.Codec().BinaryFromNative(nil, native)
		if err != nil {
			log.Fatalf("CodecBinaryFromNative: %s", err)
		}
		var recordValue []byte
		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)
		recordValue = append(recordValue, valueBytes...)

		key, _ := uuid.NewUUID()
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &Topic, Partition: kafka.PartitionAny},
			Key: []byte(key.String()), Value: recordValue}, nil)
		for {
			unFlushed := p.Flush(5)
			if unFlushed < 1000 {
				break
			}
		}
	}
	for {
		unFlushed := p.Flush(100)
		if unFlushed < 1 {
			break
		}
	}
}

func dump(data interface{}) string {
	b,_:=json.MarshalIndent(data, "", "  ")
	return string(b)
}