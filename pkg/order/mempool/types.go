package mempool

import (
	"github.com/meshplus/bitxhub-model/pb"
	raftproto "github.com/meshplus/bitxhub/pkg/order/etcdraft/proto"
	"time"
)

const (
	btreeDegree = 10
)

// TODO(YH): add to configuration item
const (
	DefaultPoolSize    = 50000
	DefaultTxCacheSize = 10000
	DefaultTxSetSize   = 10
	DefaultBlockSize   = 200

	DefaultFetchTxnTimeout = 3 * time.Second
)

type LocalMissingTxnEvent struct {
	Height             uint64
	MissingTxnHashList map[uint64]string
	WaitC              chan bool
}

type subscribeEvent struct {
	txForwardC           chan *TxSlice
	localMissingTxnEvent chan *LocalMissingTxnEvent
	fetchTxnRequestC     chan *FetchTxnRequest
	fetchTxnResponseC    chan *FetchTxnResponse
	getBlockC            chan *constructBatchEvent
	commitTxnC           chan *raftproto.Ready
	updateLeaderC        chan uint64
}

type mempoolBatch struct {
	missingTxnHashList map[uint64]string
	txList             []*pb.Transaction
}

type constructBatchEvent struct {
	ready  *raftproto.Ready
	result chan *mempoolBatch
}

