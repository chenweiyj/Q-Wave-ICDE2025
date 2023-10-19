package main

import (
	"bufio"
	. "chain/expt9"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"github.com/libp2p/go-libp2p/p2p/net/connmgr"

	"github.com/multiformats/go-multiaddr"
)

var superNode *SuperNode
var ch chan *bufio.ReadWriter

func handleStream(s network.Stream) {
	// Create a buffer stream for non-blocking read and write.
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))

	// add rw to super node
	log.Println("send rw", rw)
	ch <- rw
	log.Println("send rw done", rw)

	go superNode.ProcessStream(rw)

	// stream 's' will stay open until you close it (or the other side closes it).
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourcePort := flag.Int("sp", 0, "Source port number")
	dest := flag.String("d", "", "Destination multiaddr string")
	help := flag.Bool("help", false, "Display help")
	debug := flag.Bool("debug", true, "Debug generates the same node ID on every execution")
	clients := flag.Int("c", 70, "number of clients")
	times := flag.Int("times", 1, "number of running epochs")
	delay := flag.Int("delay", 0, "super node message sending delay")

	flag.Parse()

	if *help {
		fmt.Printf("This program demonstrates a simple p2p chain application using libp2p\n\n")
		fmt.Println("Usage: Run './chain -sp <SOURCE_PORT>' where <SOURCE_PORT> can be any port number.")
		fmt.Println("Now run './chain -d <MULTIADDR> -c <CLIENTS>' where <MULTIADDR> is multiaddress of previous listener host.")

		os.Exit(0)
	}

	// If debug is enabled, use a constant random source to generate the peer ID. Only useful for debugging,
	// off by default. Otherwise, it uses rand.Reader.
	var r io.Reader
	if *debug {
		// Use the port number as the randomness source.
		// This will always generate the same host ID on multiple executions, if the same port number is used.
		// Never do this in production code.
		r = mrand.New(mrand.NewSource(int64(*sourcePort)))
	} else {
		r = rand.Reader
	}

	if *dest == "" {
		rmgr, cmgr := createUnlimitedManager(*clients)

		h, err := makeHost(*sourcePort, r, rmgr, cmgr)
		if err != nil {
			log.Println(err)
			return
		}

		ch = make(chan *bufio.ReadWriter)
		startPeer(ctx, h, handleStream)
		superNode = createSuperNode(*clients)

		go func() {
			for range time.Tick(1 * time.Second) {
				if len(h.Network().Peers()) == *clients {
					break
				}
			}
			log.Println("start supernode process")
			superNode.Process(*times)
		}()

		// go func() {
		// 	for range time.Tick(6 * time.Second) {
		// 		log.Printf("(%v peers) %v connections, store %v peers", len(h.Network().Peers()), len(h.Network().Conns()), h.Peerstore().Peers().Len())
		// 	}
		// }()
	} else {

		controlclients(*clients, *dest, *delay)
	}

	// Wait forever
	select {}
}

func createUnlimitedManager(clients int) (network.ResourceManager, *connmgr.BasicConnMgr) {
	/* 1) Use resource manager limit */
	// limits := rcmgr.PartialLimitConfig{
	// 	System: rcmgr.ResourceLimits{
	// 		Streams:         rcmgr.LimitVal((clients) * 2),
	// 		StreamsInbound:  rcmgr.LimitVal((clients) * 2),
	// 		StreamsOutbound: rcmgr.LimitVal((clients) * 2),
	// 		Conns:           rcmgr.LimitVal((clients) * 2),
	// 		ConnsInbound:    rcmgr.LimitVal((clients) * 2),
	// 		ConnsOutbound:   rcmgr.LimitVal((clients) * 2),
	// 	},
	// 	Transient: rcmgr.ResourceLimits{
	// 		Streams:         rcmgr.LimitVal((clients) * 2),
	// 		StreamsInbound:  rcmgr.LimitVal((clients) * 2),
	// 		StreamsOutbound: rcmgr.LimitVal((clients) * 2),
	// 		Conns:           rcmgr.LimitVal((clients) * 2),
	// 		ConnsInbound:    rcmgr.LimitVal((clients) * 2),
	// 		ConnsOutbound:   rcmgr.LimitVal((clients) * 2),
	// 	},
	// }

	// limiter := rcmgr.NewFixedLimiter(limits.Build(rcmgr.DefaultLimits.AutoScale()))
	// rmgr, err := rcmgr.NewResourceManager(limiter)
	// if err != nil {
	// 	panic(err)
	// }

	/* Or 2) Unlimit */
	rmgr := &network.NullResourceManager{}

	cmgr, err := connmgr.NewConnManager(0, clients)
	if err != nil {
		panic(err)
	}

	return rmgr, cmgr
}

func makeHost(port int, randomness io.Reader, rmgr network.ResourceManager, cmgr *connmgr.BasicConnMgr) (host.Host, error) {
	// Creates a new RSA key pair for this host.
	prvKey, _, err := crypto.GenerateKeyPairWithReader(crypto.ECDSA, 192, randomness)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// 0.0.0.0 will listen on any interface device.
	sourceMultiAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port))

	// libp2p.New constructs a new libp2p Host.
	// Other options can be added here.
	return libp2p.New(
		libp2p.ResourceManager(rmgr), libp2p.ConnectionManager(cmgr),
		libp2p.ListenAddrs(sourceMultiAddr),
		libp2p.Identity(prvKey),
	)
}

func startPeer(ctx context.Context, h host.Host, streamHandler network.StreamHandler) {
	// Set a function as stream handler.
	// This function is called when a peer connects, and starts a stream with this protocol.
	// Only applies on the receiving side.
	h.SetStreamHandler("/chat/1.0.0", streamHandler)

	// Let's get the actual TCP port from our listen multiaddr, in case we're using 0 (default; random available port).
	var port string
	for _, la := range h.Network().ListenAddresses() {
		if p, err := la.ValueForProtocol(multiaddr.P_TCP); err == nil {
			port = p
			break
		}
	}

	if port == "" {
		log.Println("was not able to find actual local port")
		return
	}

	log.Printf("Run './chain -d /ip4/127.0.0.1/tcp/%v/p2p/%s' on another console.\n", port, h.ID())
	log.Println("You can replace 127.0.0.1 with public IP as well.")
	log.Println("Waiting for incoming connection")
	log.Println()
}

func createSuperNode(clientNo int) *SuperNode {
	// create a new super node, and add listened streams
	node := NewSuperNode(clientNo)
	go func() {
		for i := 0; i < clientNo; i++ {
			log.Println("get rw from channel", i, clientNo)
			rw := <-ch
			node.Streams[i] = rw
			log.Println("get rw done", rw)
		}
		log.Println("done creating rw")
		log.Println(node.Streams)
	}()

	return node
}

func startPeerAndConnect(ctx context.Context, h host.Host, destination string) (*bufio.ReadWriter, error) {
	log.Println("nodeID:", h.ID())
	// for _, la := range h.Addrs() {
	// 	log.Printf(" - %v\n", la)
	// }
	// log.Println()

	// Turn the destination into a multiaddr.
	maddr, err := multiaddr.NewMultiaddr(destination)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Extract the peer ID from the multiaddr.
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Add the destination's peer multiaddress in the peerstore.
	// This will be used during connection and stream creation by libp2p.
	h.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)

	// Start a stream with the destination.
	// Multiaddress of the destination peer is fetched from the peerstore using 'peerId'.
	s, err := h.NewStream(context.Background(), info.ID, "/chat/1.0.0")
	if err != nil {
		log.Println(err)
		return nil, err
	}
	log.Println("Established connection to destination")

	// Create a buffered stream so that read and writes are non-blocking.
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))

	return rw, nil
}

func controlclients(clients int, dest string, delay int) {
	n := atomic.Int32{}
	wg := sync.WaitGroup{}
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			time.Sleep(time.Duration(i%100) * 100 * time.Millisecond)
			defer wg.Done()
			err := newClientChat(dest, i, delay)
			if err != nil {
				panic(err)
			}
			println("Clients done:", n.Add(1))
			// println(i)
		}(i)
	}

	wg.Wait()
}

func newClientChat(dest string, clientI int, delay int) error {
	// client
	var err error
	ctx := context.Background()
	c, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		fmt.Println("Error lip2pNew")
		return err
	}

	rw, err := startPeerAndConnect(ctx, c, dest)
	if err != nil {
		log.Println(err)
		return err
	}

	// Create a thread to read and write data.
	// go writeData(rw, clientNo)
	// go readData(rw)
	clientNode := NewClientNode(rw, delay, clientI)
	go clientNode.Process()

	//c.Close()
	return nil
}
