// EVM blockchain worker
package categorizer

import (
	"fmt"

	"github.com/blocklords/gosds/blockchain/evm/abi"
	"github.com/blocklords/gosds/categorizer/event"
	"github.com/blocklords/gosds/categorizer/smartcontract"

	spaghetti_log "github.com/blocklords/gosds/blockchain/event"
)

// For EVM based smartcontracts
type EvmWorker struct {
	abi *abi.Abi

	log_parse_in  chan RequestLogParse
	log_parse_out chan ReplyLogParse

	smartcontract *smartcontract.Smartcontract
}

// Wraps the Worker with the EVM related data and returns the wrapped Worker as EvmWorker
func New(sm *smartcontract.Smartcontract, abi *abi.Abi) *EvmWorker {
	return &EvmWorker{
		abi:           abi,
		smartcontract: sm,
	}
}

// Categorize the blocks for this smartcontract
func (worker *EvmWorker) categorize(logs []*spaghetti_log.Log) uint64 {
	network_id := worker.smartcontract.NetworkId
	address := worker.smartcontract.Address

	var block_number uint64 = worker.smartcontract.CategorizedBlockNumber
	var block_timestamp uint64 = worker.smartcontract.CategorizedBlockTimestamp

	if len(logs) > 0 {
		for log_index := 0; log_index < len(logs); log_index++ {
			raw_log := logs[log_index]

			fmt.Println("requesting parse of smartcontract log to SDS Log...", raw_log, worker.smartcontract)
			worker.log_parse_in <- RequestLogParse{
				network_id: network_id,
				address:    address,
				data:       raw_log.Data,
				topics:     raw_log.Topics,
			}
			log_reply := <-worker.log_parse_out
			fmt.Println("reply received from SDS Log")
			if log_reply.err != nil {
				fmt.Println("abi.remote parse %w, we skip this log records", log_reply.err)
				continue
			}

			l := event.New(log_reply.log_name, log_reply.outputs).AddMetadata(raw_log).AddSmartcontractData(worker.smartcontract)

			if l.BlockNumber > block_number {
				block_number = l.BlockNumber
				block_timestamp = l.BlockTimestamp
			}
		}
	}

	fmt.Println("categorization finished, update the block number to ", block_number, worker.smartcontract.NetworkId, worker.smartcontract.Address)
	worker.smartcontract.SetBlockParameter(block_number, block_timestamp)

	return block_number
}
