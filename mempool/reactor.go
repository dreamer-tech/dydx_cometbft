package mempool

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	"go.uber.org/atomic"
	"os"
	"sync"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/clist"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	protomem "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/types"
	"golang.org/x/sync/semaphore"
)

// Reactor handles mempool tx broadcasting amongst peers.
// It maintains a map from peer ID to counter, to prevent gossiping txs to the
// peers you received it from.
type Reactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool *CListMempool
	ids     *mempoolIDs

	// Semaphores to keep track of how many connections to peers are active for broadcasting
	// transactions. Each semaphore has a capacity that puts an upper bound on the number of
	// connections for different groups of peers.
	activePersistentPeersSemaphore    *semaphore.Weighted
	activeNonPersistentPeersSemaphore *semaphore.Weighted

	peersMutex         *sync.Mutex
	peersRankingWriter *csv.Writer
	peersTxsWriter     *csv.Writer
	peersRank          map[string]int
	peersTxs           [][]string
	peersLastDump      atomic.Time
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(config *cfg.MempoolConfig, mempool *CListMempool) *Reactor {
	// initialize csv writer
	file, _ := os.OpenFile("~/peers_ranking.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	peersRankingWriter := csv.NewWriter(file)

	file, _ = os.OpenFile("~/peers_txs.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	peersTxsWriter := csv.NewWriter(file)

	ts := atomic.Time{}
	ts.Store(time.Now())
	memR := &Reactor{
		config:             config,
		mempool:            mempool,
		ids:                newMempoolIDs(),
		peersMutex:         &sync.Mutex{},
		peersRankingWriter: peersRankingWriter,
		peersTxsWriter:     peersTxsWriter,
		peersRank:          make(map[string]int, 0),
		peersTxs:           make([][]string, 0),
		peersLastDump:      ts,
	}
	memR.BaseReactor = *p2p.NewBaseReactor("Mempool", memR)
	memR.activePersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers))
	memR.activeNonPersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers))

	return memR
}

// InitPeer implements Reactor by creating a state for the peer.
func (memR *Reactor) InitPeer(peer p2p.Peer) p2p.Peer {
	memR.ids.ReserveForPeer(peer)
	return peer
}

// SetLogger sets the Logger on the reactor and the underlying mempool.
func (memR *Reactor) SetLogger(l log.Logger) {
	memR.Logger = l
	memR.mempool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (memR *Reactor) OnStart() error {
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	}
	return nil
}

// GetChannels implements Reactor by returning the list of channels for this
// reactor.
func (memR *Reactor) GetChannels() []*p2p.ChannelDescriptor {
	largestTx := make([]byte, memR.config.MaxTxBytes)
	batchMsg := protomem.Message{
		Sum: &protomem.Message_Txs{
			Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
		},
	}

	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: batchMsg.Size(),
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *Reactor) AddPeer(peer p2p.Peer) {
	if memR.config.Broadcast {
		go func() {
			// Always forward transactions to unconditional peers.
			if !memR.Switch.IsPeerUnconditional(peer.ID()) {
				// Depending on the type of peer, we choose a semaphore to limit the gossiping peers.
				var peerSemaphore *semaphore.Weighted
				if peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers > 0 {
					peerSemaphore = memR.activePersistentPeersSemaphore
				} else if !peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers > 0 {
					peerSemaphore = memR.activeNonPersistentPeersSemaphore
				}

				if peerSemaphore != nil {
					for peer.IsRunning() {
						// Block on the semaphore until a slot is available to start gossiping with this peer.
						// Do not block indefinitely, in case the peer is disconnected before gossiping starts.
						ctxTimeout, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
						// Block sending transactions to peer until one of the connections become
						// available in the semaphore.
						err := peerSemaphore.Acquire(ctxTimeout, 1)
						cancel()

						if err != nil {
							continue
						}

						// Release semaphore to allow other peer to start sending transactions.
						defer peerSemaphore.Release(1)
						break
					}
				}
			}

			memR.mempool.metrics.ActiveOutboundConnections.Add(1)
			defer memR.mempool.metrics.ActiveOutboundConnections.Add(-1)
			memR.broadcastTxRoutine(peer)
		}()
	}
}

// RemovePeer implements Reactor.
func (memR *Reactor) RemovePeer(peer p2p.Peer, _ interface{}) {
	memR.ids.Reclaim(peer)
	// broadcast routine checks if peer is gone and returns
}

func (memR *Reactor) UpdatePeers(tx types.Tx, e p2p.Envelope) {
	// code taken from dydx_helpers/IsShortTermClobOrderTransaction
	cosmosTx := &cosmostx.Tx{}
	err := cosmosTx.Unmarshal(tx)
	if err != nil || cosmosTx.Body == nil || len(cosmosTx.Body.Messages) != 1 {
		return
	}
	if cosmosTx.Body.Messages[0].TypeUrl == "/dydxprotocol.clob.MsgPlaceOrder" ||
		cosmosTx.Body.Messages[0].TypeUrl == "/dydxprotocol.clob.MsgCancelOrder" ||
		cosmosTx.Body.Messages[0].TypeUrl == "/dydxprotocol.clob.MsgBatchCancel" {

		memR.peersMutex.Lock()
		defer memR.peersMutex.Unlock()

		if rank, exist := memR.peersRank[e.Src.RemoteAddr().String()]; exist {
			memR.peersRank[e.Src.RemoteAddr().String()] = rank + 1
		} else {
			memR.peersRank[e.Src.RemoteAddr().String()] = 1
		}

		memR.peersTxs = append(memR.peersTxs, []string{
			fmt.Sprintf("%d", time.Now().UnixNano()),
			hex.EncodeToString(tx.Hash()),
			e.Src.RemoteAddr().String(),
		})
	}
}

func (memR *Reactor) DumpPeers() {
	lastDump := memR.peersLastDump.Load()
	if time.Now().Sub(lastDump) < 5*time.Minute {
		return
	}

	memR.peersMutex.Lock()
	defer memR.peersMutex.Unlock()

	res := ""
	for ipAddr, rank := range memR.peersRank {
		res += fmt.Sprintf("%s=%d@", ipAddr, rank)
	}

	err := memR.peersRankingWriter.Write([]string{
		fmt.Sprintf("%d", time.Now().UnixNano()),
		res,
	})
	if err != nil {
		memR.Logger.Error(fmt.Sprintf("Write peers csv error: %s\n", err.Error()))
	}
	memR.peersRankingWriter.Flush()

	err = memR.peersTxsWriter.WriteAll(memR.peersTxs)
	if err != nil {
		memR.Logger.Error(fmt.Sprintf("Write peers csv error: %s\n", err.Error()))
	}
	memR.peersTxsWriter.Flush()

	memR.peersTxs = make([][]string, 0)
	memR.peersLastDump.Store(time.Now())
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *Reactor) Receive(e p2p.Envelope) {
	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
	switch msg := e.Message.(type) {
	case *protomem.Txs:
		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("received empty txs from peer", "src", e.Src)
			return
		}
		txInfo := TxInfo{SenderID: memR.ids.GetForPeer(e.Src)}
		if e.Src != nil {
			txInfo.SenderP2PID = e.Src.ID()
		}

		var err error
		for _, tx := range protoTxs {
			ntx := types.Tx(tx)
			err = memR.mempool.CheckTx(ntx, nil, txInfo)
			if errors.Is(err, ErrTxInCache) {
				memR.Logger.Debug("Tx already exists in cache", "tx", ntx.String())
			} else if err != nil {
				memR.Logger.Debug("Could not check tx", "tx", ntx.String(), "err", err)
			}
			if err == nil {
				memR.UpdatePeers(ntx, e)
				memR.DumpPeers()
			}
		}
	default:
		memR.Logger.Error("unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}

	// broadcasting happens from go routines per peer
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// Send new mempool txs to peer.
func (memR *Reactor) broadcastTxRoutine(peer p2p.Peer) {
	peerID := memR.ids.GetForPeer(peer)
	var next *clist.CElement

	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !memR.IsRunning() || !peer.IsRunning() {
			return
		}

		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-memR.mempool.TxsWaitChan(): // Wait until a tx is available
				if next = memR.mempool.TxsFront(); next == nil {
					continue
				}
			case <-peer.Quit():
				return
			case <-memR.Quit():
				return
			}
		}

		// Make sure the peer is up to date.
		peerState, ok := peer.Get(types.PeerStateKey).(PeerState)
		if !ok {
			// Peer does not have a state yet. We set it in the consensus reactor, but
			// when we add peer in Switch, the order we call reactors#AddPeer is
			// different every time due to us using a map. Sometimes other reactors
			// will be initialized before the consensus reactor. We should wait a few
			// milliseconds and retry.
			time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		// Allow for a lag of 1 block.
		memTx := next.Value.(*mempoolTx)
		if peerState.GetHeight() < memTx.Height()-1 {
			time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		// NOTE: Transaction batching was disabled due to
		// https://github.com/tendermint/tendermint/issues/5796

		if !memTx.isSender(peerID) {
			success := peer.Send(p2p.Envelope{
				ChannelID: MempoolChannel,
				Message:   &protomem.Txs{Txs: [][]byte{memTx.tx}},
			})
			if !success {
				time.Sleep(PeerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}
		}

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-peer.Quit():
			return
		case <-memR.Quit():
			return
		}
	}
}

// TxsMessage is a Message containing transactions.
type TxsMessage struct {
	Txs []types.Tx
}

// String returns a string representation of the TxsMessage.
func (m *TxsMessage) String() string {
	return fmt.Sprintf("[TxsMessage %v]", m.Txs)
}
