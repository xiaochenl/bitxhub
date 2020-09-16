package mempool

import (
	"encoding/hex"
	"errors"
	"github.com/meshplus/bitxhub-kit/types"
	"github.com/meshplus/bitxhub-model/pb"
	raftproto "github.com/meshplus/bitxhub/pkg/order/etcdraft/proto"
	"sync/atomic"
)

func (mpi *mempoolImpl) getBatchSeqNo() uint64 {
	return atomic.LoadUint64(&mpi.batchSeqNo)
}

func (mpi *mempoolImpl) increaseBatchSeqNo() {
	atomic.AddUint64(&mpi.batchSeqNo, 1)
}

// getTxByTxPointer returns the tx stored in allTxs by given TxPointer.
func (mpi *mempoolImpl) getTxByTxPointer(txPointer orderedQueueKey) *pb.Transaction {
	if txnMap, ok := mpi.txStore.allTxs[txPointer.account]; ok {
		return txnMap.getTxByNonce(txPointer.seqNo)
	}
	return nil
}

func (mpi *mempoolImpl) msgToConsensusPbMsg(data []byte, tyr raftproto.RaftMessage_Type) *pb.Message {
	rm := &raftproto.RaftMessage{
		Type:   tyr,
		FromId: mpi.localID,
		Data:   data,
	}
	cmData, err := rm.Marshal()
	if err != nil {
		return nil
	}
	msg := &pb.Message{
		Type: pb.Message_CONSENSUS,
		Data: cmData,
	}
	return msg
}

func newSubscribe() *subscribeEvent {
	return &subscribeEvent{
		txForwardC:           make(chan *TxSlice),
		localMissingTxnEvent: make(chan *LocalMissingTxnEvent),
		fetchTxnRequestC:     make(chan *FetchTxnRequest),
		updateLeaderC:        make(chan uint64),
		fetchTxnResponseC:    make(chan *FetchTxnResponse),
		commitTxnC:           make(chan *raftproto.Ready),
		getBlockC:            make(chan *constructBatchEvent),
	}
}

// TODO(YH): restore commitNonce and pendingNonce from db.
func newNonceCache() *nonceCache {
	return &nonceCache{
		commitNonces:  make(map[string]uint64),
		pendingNonces: make(map[string]uint64),
	}
}

func hex2Hash(hash string) (types.Hash, error) {
	var (
		hubHash   types.Hash
		hashBytes []byte
		err       error
	)
	if hashBytes, err = hex.DecodeString(hash); err != nil {
		return types.Hash{}, err
	}
	if len(hashBytes) != types.HashLength {
		return types.Hash{}, errors.New("invalid tx hash")
	}
	copy(hubHash[:], hashBytes)
	return hubHash, nil
}

func (mpi *mempoolImpl) poolIsFull() bool {
	return atomic.LoadUint64(&mpi.txStore.poolSize) >= DefaultPoolSize
}

func (mpi *mempoolImpl) isLeader() bool {
	return mpi.leader == mpi.localID
}
