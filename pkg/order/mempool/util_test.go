package mempool

import (
	"fmt"
	"testing"

	"github.com/meshplus/bitxhub-model/pb"

	"github.com/stretchr/testify/assert"
)

func TestGetAccount(t *testing.T) {
	ast := assert.New(t)
	privKey := genPrivKey()
	address, _ := privKey.PublicKey().Address()
	tx := constructIBTPTx(uint64(1), &privKey)
	addr, err := getAccount(tx)
	ast.Nil(err)
	expectedAddr := fmt.Sprintf("%s-%s-%d", address, address, pb.IBTP_INTERCHAIN)
	ast.Equal(expectedAddr, addr)
	tx = &pb.Transaction{
		From: InterchainContractAddr,
		To:   InterchainContractAddr,
	}
	addr, err = getAccount(tx)
	ast.Nil(err)
	ast.Equal(addr,InterchainContractAddr.String())
}

func TestPoolIsFull(t *testing.T) {
	ast := assert.New(t)
	mpi, _ := mockMempoolImpl()
	defer cleanTestData()

	isFull := mpi.poolIsFull()
	ast.Equal(false, isFull)
	mpi.txStore.poolSize = DefaultPoolSize
	isFull = mpi.poolIsFull()
	ast.Equal(true, isFull)
}
