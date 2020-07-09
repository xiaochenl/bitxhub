package contracts

import (
	"fmt"

	"github.com/meshplus/bitxhub/pkg/vm/boltvm"
)

const (
	PREFIX  = "tx-"
	BEGIN   = "BEGIN"
	SUCCESS = "SUCCESS"
	FAIL    = "FAIL"
)

type TransactionManager struct {
	boltvm.Stub
}

type TransactionInfo struct {
	globalState string
	childTxInfo map[string]string
}

func (t *TransactionManager) BeginMultiTXs(globalId string, childTxIds ...string) *boltvm.Response {
	if t.Has(t.txInfoKey(globalId)) {
		return boltvm.Error("Transaction id already exists")
	}

	txInfo := &TransactionInfo{
		globalState: BEGIN,
		childTxInfo: make(map[string]string),
	}

	for _, childTxId := range childTxIds {
		txInfo.childTxInfo[childTxId] = BEGIN
		t.Set(childTxId, []byte(globalId))
	}

	t.SetObject(t.txInfoKey(globalId), txInfo)

	return boltvm.Success(nil)
}

func (t *TransactionManager) Begin(txId string) *boltvm.Response {
	if t.Has(t.txInfoKey(txId)) {
		return boltvm.Error("Transaction id already exists")
	}

	t.Set(t.txInfoKey(txId), []byte(BEGIN))

	return boltvm.Success(nil)
}

func (t *TransactionManager) Report(txId string, result int32) *boltvm.Response {
	ok, val := t.Get(t.txInfoKey(txId))
	if ok {
		status := string(val)
		if status != BEGIN {
			return boltvm.Error(fmt.Sprintf("transaction with Id %s is finished", txId))
		}

		if result == 0 {
			t.Set(t.txInfoKey(txId), []byte(SUCCESS))
		} else {
			t.Set(t.txInfoKey(txId), []byte(FAIL))
		}
	} else {
		ok, val = t.Get(txId)
		if !ok {
			return boltvm.Error(fmt.Sprintf("cannot get global id of child tx id %s", txId))
		}

		globalId := string(val)
		txInfo := &TransactionInfo{}
		if !t.GetObject(t.txInfoKey(globalId), &txInfo) {
			return boltvm.Error(fmt.Sprintf("transaction global id %s does not exist", globalId))
		}

		if txInfo.globalState != BEGIN {
			return boltvm.Error(fmt.Sprintf("transaction with global Id %s is finished", globalId))
		}

		status, ok := txInfo.childTxInfo[txId]
		if !ok {
			return boltvm.Error(fmt.Sprintf("%s is not in transaction %s", txId, globalId))
		}

		if status != BEGIN {
			return boltvm.Error(fmt.Sprintf("%s has already reported result", txId))
		}

		if result == 0 {
			txInfo.childTxInfo[txId] = SUCCESS
			count := 0
			for _, res := range txInfo.childTxInfo {
				if res != SUCCESS {
					break
				}
				count++
			}

			if count == len(txInfo.childTxInfo) {
				txInfo.globalState = SUCCESS
			}

		} else {
			txInfo.childTxInfo[txId] = FAIL
			txInfo.globalState = FAIL
		}

		t.SetObject(t.txInfoKey(globalId), txInfo)
	}

	return boltvm.Success(nil)
}

func (t *TransactionManager) txInfoKey(id string) string {
	return fmt.Sprintf("%s-%s", PREFIX, id)
}
