package mempool

import (
	"github.com/google/btree"
	"github.com/meshplus/bitxhub-model/pb"
)

// TODO(YH): add expiration time order
type orderedQueueKey struct {
	account string
	seqNo   uint64
}

// Less should guarantee item can be cast into *TxPointer
func (tp *orderedQueueKey) Less(than btree.Item) bool {
	other := than.(*orderedQueueKey)
	if tp.account != other.account {
		return tp.account < other.account
	}
	return tp.seqNo < other.seqNo
}


type NonceSortedKey struct {
	Nonce uint64
}

// Less should guarantee item can be cast into *NonceSortedKey
func (src *NonceSortedKey) Less(item btree.Item) bool {
	dst, _ := item.(*NonceSortedKey)

	return src.Nonce < dst.Nonce
}

func makeOrderedQueueKey(tx *pb.Transaction) *orderedQueueKey {
	return &orderedQueueKey{
		account: tx.From.Hex(),
		seqNo:   uint64(tx.Nonce),
	}
}

func makeTxKey(nonce uint64) *NonceSortedKey {
	return &NonceSortedKey{
		Nonce:   nonce,
	}
}

type btreeIndex struct {
	data *btree.BTree
}

func newBtreeIndex() *btreeIndex {
	return &btreeIndex{
		data: btree.New(btreeDegree),
	}
}

func (idx *btreeIndex) insert(tx *pb.Transaction) {
	idx.data.ReplaceOrInsert(makeTxKey(uint64(tx.Nonce)))
}

func (idx *btreeIndex) remove(txs []*pb.Transaction) {
	for _, tx := range txs {
		idx.data.Delete(makeTxKey(uint64(tx.Nonce)))
	}
}

func (idx *btreeIndex) insertByOrderedQueueKey(tx *pb.Transaction) {
	idx.data.ReplaceOrInsert(makeOrderedQueueKey(tx))
}

func (idx *btreeIndex) removeByOrderedQueueKey(txs []*pb.Transaction) {
	for _, tx := range txs {
		idx.data.Delete(makeOrderedQueueKey(tx))
	}
}

// Size returns the size of the index
func (idx *btreeIndex) size() uint64 {
	return uint64(idx.data.Len())
}

