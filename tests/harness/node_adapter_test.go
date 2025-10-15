package harness_test

import (
    hnet "github.com/sevenDatabase/SevenDB/internal/harness/network"
)

// RealNode defines the minimal subset of your real node APIs we need to adapt.
// Replace these methods with your concrete server node APIs.
type RealNode interface {
    ID() string
    Start()
    Stop()
    HandleMessage(from string, msg []byte)
    ClientSet(key, value string)
    ClientSubscribe(key string)
    StateHash() []byte
}

// NodeAdapter wraps a RealNode and satisfies harness.Node.
type NodeAdapter struct{ n RealNode }

func NewNodeAdapter(n RealNode) *NodeAdapter { return &NodeAdapter{n: n} }

func (a *NodeAdapter) ID() hnet.NodeID { return hnet.NodeID(a.n.ID()) }
func (a *NodeAdapter) Start()          { a.n.Start() }
func (a *NodeAdapter) Stop()           { a.n.Stop() }
func (a *NodeAdapter) HandleMessage(from hnet.NodeID, msg hnet.Message) {
    a.n.HandleMessage(string(from), []byte(msg))
}
func (a *NodeAdapter) ClientSet(key, value string) { a.n.ClientSet(key, value) }
func (a *NodeAdapter) ClientSubscribe(key string)  { a.n.ClientSubscribe(key) }
func (a *NodeAdapter) StateHash() []byte           { return a.n.StateHash() }

// Example wiring (pseudo; replace with your node creation):
// func Test_Adapter_Wiring(t *testing.T) {
//     harn := harness.New(nil, nil, nil)
//     rn := newYourRealNode()
//     harn.RegisterNode(NewNodeAdapter(rn))
//     script := h.Script{Actions: []h.TimedAction{
//         h.StartNode(hnet.NodeID(rn.ID()))(0),
//         h.ClientSet(hnet.NodeID(rn.ID()), "k", "v")(0),
//     }}
//     script.Run(harn)
// }
