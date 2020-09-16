package mempool

import (
	"github.com/meshplus/bitxhub-model/pb"
	"github.com/meshplus/bitxhub/internal/loggers"
	"github.com/sirupsen/logrus"
)

type TxCache struct {
	recvTxC chan *pb.Transaction
	txSetC  chan *TxSlice
	txSet   []*pb.Transaction
	logger  logrus.FieldLogger

	// TODO(YH): add timer for txSet
	timerC chan bool
	close  chan bool
}

func newTxCache() *TxCache {
	txCache := &TxCache{}
	txCache.recvTxC = make(chan *pb.Transaction, DefaultTxCacheSize)
	txCache.close = make(chan bool)
	txCache.txSetC = make(chan *TxSlice)
	txCache.timerC = make(chan bool)
	txCache.txSet = make([]*pb.Transaction,0)
	txCache.logger = loggers.Logger(loggers.Order)
	return txCache
}

func (tc *TxCache) listenEvent() {
	for {
		select {
		case <-tc.close:
			tc.logger.Info("Exit transaction cache")

		case tx := <-tc.recvTxC:
			tc.appendTx(tx)
		}
	}
}

func (tc *TxCache) appendTx(tx *pb.Transaction) {
	if tx == nil {
		tc.logger.Errorf("Transaction is nil")
		return
	}
	tc.txSet = append(tc.txSet, tx)
	if len(tc.txSet) >= DefaultTxSetSize {
		dst := make([]*pb.Transaction, len(tc.txSet))
		copy(dst, tc.txSet)
		txSet := &TxSlice{
			TxList: dst,
		}
		tc.txSetC <- txSet
		tc.txSet = make([]*pb.Transaction,0)
	}
}

func (tc *TxCache) IsFull() bool {
	return len(tc.recvTxC) == DefaultTxCacheSize
}
