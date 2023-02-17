/*Spaghetti transaction without method name and without clear input parameters*/
package log

import (
	"encoding/json"
	"fmt"

	"github.com/blocklords/gosds/common/data_type/key_value"
)

type Log struct {
	NetworkId      string   `json:"network_id"`
	Txid           string   `json:"txid"`             // txId column
	BlockNumber    uint64   `json:"block_number"`     // block
	BlockTimestamp uint64   `json:"block_timestamp"`  // block
	LogIndex       uint     `json:"log_index"`        // index
	Data           string   `json:"data"`             // datatext data type
	Topics         []string `json:"topics,omitempty"` // topics
	Address        string   `json:"address"`          // address
}

// JSON string representation of the spaghetti.Log
func (l *Log) ToString() (string, error) {
	kv, err := key_value.NewFromInterface(l)
	if err != nil {
		return "", fmt.Errorf("failed to serialize spaghetti log to intermediate key-value %v: %v", l, err)
	}

	bytes, err := kv.ToBytes()
	if err != nil {
		return "", fmt.Errorf("failed to serialize intermediate key-value to string %v: %v", l, err)
	}

	return string(bytes), nil
}

// Serielizes the Log.Topics into the byte array
func (b *Log) TopicRaw() []byte {
	byt, err := json.Marshal(b.Topics)
	if err != nil {
		return []byte{}
	}

	return byt
}

// Converts the byte series into the topic list
func (b *Log) ParseTopics(raw []byte) error {
	var topics []string
	err := json.Unmarshal(raw, &topics)
	if err != nil {
		return err
	}
	b.Topics = topics

	return nil
}
