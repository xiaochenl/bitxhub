package mempool

import (
	"github.com/google/btree"
	"github.com/meshplus/bitxhub-model/pb"
)

type transactionStore struct {
	// track all valid tx hashes cached in mempool
	txHashMap map[string]*orderedQueueKey
	// track all valid tx, mapping user' account to all related transactions.
	allTxs map[string]*txSortedMap
	// track the commit nonce and pending nonce of each account.
	nonceCache *nonceCache
	// keeps track of "non-ready" txs (txs that can't be included in next block)
	// only used to help remove some txs if pool is full.
	parkingLotIndex *btreeIndex
	// keeps track of "ready" txs
	priorityIndex *btreeIndex
	// cache all the batched txs which haven't executed.
	batchedTxs map[orderedQueueKey]bool
	// cache all batches created by current primary in order, removed after they are been executed.
	// TODO(YH): change the type of key from height to digest.
	batchedCache map[uint64][]*pb.Transaction
	// trace the missing transaction
	missingBatch map[uint64]map[uint64]string
	// track the current size of mempool
	poolSize uint64
	// track the non-batch prioritySize
	prioritySize uint64
}

func newTransactionStore() *transactionStore {
	return &transactionStore{
		txHashMap:       make(map[string]*orderedQueueKey, 0),
		allTxs:          make(map[string]*txSortedMap),
		batchedTxs:      make(map[orderedQueueKey]bool),
		missingBatch:    make(map[uint64]map[uint64]string),
		batchedCache:    make(map[uint64][]*pb.Transaction),
		parkingLotIndex: newBtreeIndex(),
		priorityIndex:   newBtreeIndex(),
		nonceCache:      newNonceCache(),
	}
}

// Get transaction by account address + nonce
func (txStore *transactionStore) getTxByOrderKey(account string, seqNo uint64) *pb.Transaction {
	if list, ok := txStore.allTxs[account]; ok {
		res := list.getTxByNonce(seqNo)
		if res == nil {
			return nil
		}
		return res
	}
	return nil
}

type txSortedMap struct {
	items map[uint64]*pb.Transaction // map nonce to transaction
	index *btreeIndex                // index for items
}

func newTxSortedMap() *txSortedMap {
	return &txSortedMap{
		items: make(map[uint64]*pb.Transaction),
		index: newBtreeIndex(),
	}
}

func (m *txSortedMap) filterReady(demandNonce uint64) ([]*pb.Transaction, []*pb.Transaction, uint64) {
	var readyTxs, nonReadyTxs []*pb.Transaction
	if m.index.data.Len() == 0 {
		return nil, nil, demandNonce
	}
	demandKey := makeTxKey(demandNonce)
	m.index.data.AscendGreaterOrEqual(demandKey, func(i btree.Item) bool {
		nonce := i.(*NonceSortedKey).Nonce
		if nonce == demandNonce {
			readyTxs = append(readyTxs, m.items[demandNonce])
			demandNonce++
		} else {
			nonReadyTxs = append(nonReadyTxs, m.items[nonce])
		}
		return true
	})

	return readyTxs, nonReadyTxs, demandNonce
}

// getTxByNonce gets retrieves the current allTxs associated with the given nonce.
func (m *txSortedMap) getTxByNonce(nonce uint64) *pb.Transaction {
	return m.items[nonce]
}

// Forward removes all allTxs from the map with a nonce lower than the
// provided commitNonce. Every removed transaction is returned for any post-removal
// maintenance.
func (m *txSortedMap) Forward(commitNonce uint64) []*pb.Transaction {
	var removedTxs []*pb.Transaction
	commitNonceKey := makeTxKey(commitNonce)
	m.index.data.AscendLessThan(commitNonceKey, func(i btree.Item) bool {
		// delete tx from map.
		nonce := i.(*NonceSortedKey).Nonce
		removedTxs = append(removedTxs, m.items[nonce])
		delete(m.items, nonce)
		return true
	})
	return removedTxs
}

type nonceCache struct {
	// commitNonces records each account's latest committed nonce in ledger.
	commitNonces map[string]uint64
	// pendingNonces records each account's latest nonce which has been included in
	// priority queue. Invariant: pendingNonces[account] >= commitNonces[account]
	pendingNonces map[string]uint64
}

// todo yh 这边应该是<=,但是premo从0开始赋值会报错
func (nc *nonceCache) getCommitNonce(account string) uint64 {
	nonce, ok := nc.commitNonces[account]
	if !ok {
		return 1
	}
	return nonce
}

func (nc *nonceCache) setCommitNonce(account string, nonce uint64) {
	nc.commitNonces[account] = nonce
}

func (nc *nonceCache) getPendingNonce(account string) uint64 {
	nonce, ok := nc.pendingNonces[account]
	if !ok {
		return 1
	}
	return nonce
}

func (nc *nonceCache) setPendingNonce(account string, nonce uint64) {
	nc.pendingNonces[account] = nonce
}
