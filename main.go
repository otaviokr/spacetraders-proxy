package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/otaviokr/spacetraders-proxy/web"
	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

func main() {
	topicRead := os.Getenv("TOPIC_READ")
	topicWrite := os.Getenv("TOPIC_WRITE")
	token := os.Getenv("TOKEN")
	kafkaConnection := fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOSTNAME"), os.Getenv("KAFKA_PORT"))
	kafkaProtocol := os.Getenv("KAFKA_PROTOCOL")

	partitionReadStr := os.Getenv("PARTITION_READ")
	partitionRead, err := strconv.Atoi(partitionReadStr)
	if err != nil {
		panic(err)
	}

	partitionWriteStr := os.Getenv("PARTITION_WRITE")
	partitionWrite, err := strconv.Atoi(partitionWriteStr)
	if err != nil {
		panic(err)
	}

	producer, err := kafka.DialLeader(context.Background(), kafkaProtocol, kafkaConnection, topicWrite, partitionWrite)
	if err != nil {
		log.Fatal("failed to dial leader for producer:", err)
	}

	consumer, err := kafka.DialLeader(context.Background(), kafkaProtocol, kafkaConnection, topicRead, partitionRead)
	if err != nil {
		log.Fatal("failed to dial leader for consumer:", err)
	}
	defer func() {
		if consumer != nil {
			if err := consumer.Close(); err != nil {
				log.Fatal("failed to close writer:", err)
			}
		}
	}()

	wp := web.NewWebProxy(token)

	for {
		// Read from Kafka a new request to process.
		log.Printf("Attempt to read message at topic: %s ...\n", topicRead)
		consumer.SetReadDeadline(time.Now().Add(1 * time.Second))
		msg, err := consumer.ReadMessage(1e6)
		if err != nil {
			log.Printf("read error: %+v\n", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		log.Printf("Message: %s\nDetails: %+v\n", string(msg.Value), msg)

		// Translate the request.
		var order map[string]string
		decoder := yaml.NewDecoder(bytes.NewReader(msg.Value))
		if err = decoder.Decode(&order); err != nil {
			log.Printf("Failed to translate request: %+v\n", err)
			// TODO Send response alarming about error!
			continue
		}

		// Process the request.
		var payload []byte
		switch order["action"] {
		case "GetShipDetails":
			payload, err = wp.GetShipInfo(order["id"])

		case "GetMarketplaceInfo":
			payload, err = wp.GetMarketplaceProducts(order["location"])

		case "PostFlightPlanNew":
			payload, err = wp.SetNewFlightPlan(order["shipId"], order["destination"])

		case "GetFlightPlanDetails":
			payload, err = wp.GetFlightPlan(order["planId"])

		case "PostBuyOrderNew":
			var quantity int
			quantity, err = strconv.Atoi(order["quantity"])
			if err != nil {
				log.Printf("Failed to translate request: %+v\n", err)
				// TODO Send response alarming about error!
				continue
			}
			payload, err = wp.BuyGood(order["shipId"], order["good"], quantity)

		case "PostSellOrderNew":
			var quantity int
			quantity, err = strconv.Atoi(order["quantity"])
			if err != nil {
				log.Printf("Failed to translate request: %+v\n", err)
				// TODO Send response alarming about error!
				continue
			}
			payload, err = wp.SellGood(order["shipId"], order["good"], quantity)

		default:
			payload = []byte("{}")
			err = fmt.Errorf("invalid order")
		}

		log.Printf("** Payload for %s: %s\n\n", order["action"], string(payload))
		if err != nil {
			log.Printf("Failed to translate request: %+v\n", err)
			// TODO Send response alarming about error!
			continue
		}

		var key string
		if _, ok := order["shipId"]; ok {
			key = order["shipId"]
		} else if _, ok := order["id"]; ok {
			key = order["id"]
		} else {
			key = "NA"
		}

		producer.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, err = producer.WriteMessages(kafka.Message{Key: []byte(key), Value: payload})
		if err != nil {
			log.Fatal("Could not produce message for Kafka:", err)
		}

		time.Sleep(500 * time.Millisecond)
	}
}
