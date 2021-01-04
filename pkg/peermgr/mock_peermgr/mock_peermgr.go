// Code generated by MockGen. DO NOT EDIT.
// Source: peermgr.go

// Package mock_peermgr is a generated GoMock package.
package mock_peermgr

import (
	event "github.com/ethereum/go-ethereum/event"
	gomock "github.com/golang/mock/gomock"
	peer "github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/meshplus/bitxhub-model/pb"
	events "github.com/meshplus/bitxhub/internal/model/events"
	network "github.com/meshplus/go-lightp2p"
	reflect "reflect"
)

// MockPeerManager is a mock of PeerManager interface
type MockPeerManager struct {
	ctrl     *gomock.Controller
	recorder *MockPeerManagerMockRecorder
}

func (m *MockPeerManager) CountConnectedPeers() uint64 {
	return 0
}

// MockPeerManagerMockRecorder is the mock recorder for MockPeerManager
type MockPeerManagerMockRecorder struct {
	mock *MockPeerManager
}

// NewMockPeerManager creates a new mock instance
func NewMockPeerManager(ctrl *gomock.Controller) *MockPeerManager {
	mock := &MockPeerManager{ctrl: ctrl}
	mock.recorder = &MockPeerManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockPeerManager) EXPECT() *MockPeerManagerMockRecorder {
	return m.recorder
}

// Start mocks base method
func (m *MockPeerManager) Start() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start")
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start
func (mr *MockPeerManagerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockPeerManager)(nil).Start))
}

// Stop mocks base method
func (m *MockPeerManager) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop
func (mr *MockPeerManagerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockPeerManager)(nil).Stop))
}

// AsyncSend mocks base method
func (m *MockPeerManager) AsyncSend(arg0 uint64, arg1 *pb.Message) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AsyncSend", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// AsyncSend indicates an expected call of AsyncSend
func (mr *MockPeerManagerMockRecorder) AsyncSend(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AsyncSend", reflect.TypeOf((*MockPeerManager)(nil).AsyncSend), arg0, arg1)
}

// SendWithStream mocks base method
func (m *MockPeerManager) SendWithStream(arg0 network.Stream, arg1 *pb.Message) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendWithStream", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendWithStream indicates an expected call of SendWithStream
func (mr *MockPeerManagerMockRecorder) SendWithStream(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendWithStream", reflect.TypeOf((*MockPeerManager)(nil).SendWithStream), arg0, arg1)
}

// Send mocks base method
func (m *MockPeerManager) Send(arg0 uint64, arg1 *pb.Message) (*pb.Message, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", arg0, arg1)
	ret0, _ := ret[0].(*pb.Message)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Send indicates an expected call of Send
func (mr *MockPeerManagerMockRecorder) Send(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockPeerManager)(nil).Send), arg0, arg1)
}

// Broadcast mocks base method
func (m *MockPeerManager) Broadcast(arg0 *pb.Message) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Broadcast", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Broadcast indicates an expected call of Broadcast
func (mr *MockPeerManagerMockRecorder) Broadcast(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Broadcast", reflect.TypeOf((*MockPeerManager)(nil).Broadcast), arg0)
}

// CountConnectedPeers mocks base method
func (m *MockPeerManager) CountConnectedPeers() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountConnectedPeers")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// CountConnectedPeers indicates an expected call of CountConnectedPeers
func (mr *MockPeerManagerMockRecorder) CountConnectedPeers() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountConnectedPeers", reflect.TypeOf((*MockPeerManager)(nil).CountConnectedPeers))
}

// Peers mocks base method
func (m *MockPeerManager) Peers() map[uint64]*pb.VpInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Peers")
	ret0, _ := ret[0].(map[uint64]*pb.VpInfo)
	return ret0
}

// Peers indicates an expected call of Peers
func (mr *MockPeerManagerMockRecorder) Peers() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Peers", reflect.TypeOf((*MockPeerManager)(nil).Peers))
}

// OtherPeers mocks base method
func (m *MockPeerManager) OtherPeers() map[uint64]*peer.AddrInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OtherPeers")
	ret0, _ := ret[0].(map[uint64]*peer.AddrInfo)
	return ret0
}

// OtherPeers indicates an expected call of OtherPeers
func (mr *MockPeerManagerMockRecorder) OtherPeers() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OtherPeers", reflect.TypeOf((*MockPeerManager)(nil).OtherPeers))
}

// SubscribeOrderMessage mocks base method
func (m *MockPeerManager) SubscribeOrderMessage(ch chan<- events.OrderMessageEvent) event.Subscription {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeOrderMessage", ch)
	ret0, _ := ret[0].(event.Subscription)
	return ret0
}

// SubscribeOrderMessage indicates an expected call of SubscribeOrderMessage
func (mr *MockPeerManagerMockRecorder) SubscribeOrderMessage(ch interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SubscribeOrderMessage", reflect.TypeOf((*MockPeerManager)(nil).SubscribeOrderMessage), ch)
}

// AddNode mocks base method
func (m *MockPeerManager) AddNode(newNodeID uint64, vpInfo *pb.VpInfo) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AddNode", newNodeID, vpInfo)
}

// AddNode indicates an expected call of AddNode
func (mr *MockPeerManagerMockRecorder) AddNode(newNodeID, vpInfo interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddNode", reflect.TypeOf((*MockPeerManager)(nil).AddNode), newNodeID, vpInfo)
}

// DelNode mocks base method
func (m *MockPeerManager) DelNode(delID uint64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DelNode", delID)
}

// DelNode indicates an expected call of DelNode
func (mr *MockPeerManagerMockRecorder) DelNode(delID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DelNode", reflect.TypeOf((*MockPeerManager)(nil).DelNode), delID)
}

// UpdateRouter mocks base method
func (m *MockPeerManager) UpdateRouter(vpInfos map[uint64]*pb.VpInfo, isNew bool) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateRouter", vpInfos, isNew)
	ret0, _ := ret[0].(bool)
	return ret0
}

// UpdateRouter indicates an expected call of UpdateRouter
func (mr *MockPeerManagerMockRecorder) UpdateRouter(vpInfos, isNew interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateRouter", reflect.TypeOf((*MockPeerManager)(nil).UpdateRouter), vpInfos, isNew)
}

// Disconnect mocks base method
func (m *MockPeerManager) Disconnect(vpInfos map[uint64]*pb.VpInfo) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Disconnect", vpInfos)
}

// Disconnect indicates an expected call of Disconnect
func (mr *MockPeerManagerMockRecorder) Disconnect(vpInfos interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Disconnect", reflect.TypeOf((*MockPeerManager)(nil).Disconnect), vpInfos)
}
