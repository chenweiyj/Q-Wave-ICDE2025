package expt2

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	. "chain/util/const"
	message "chain/util/message"
)

type SendMessage struct {
	m int
	s string
}

type SuperNode struct {
	Streams     []*bufio.ReadWriter
	clientNo    int
	groupCount  int
	clientGroup [][]int
	doneChan    chan int
	// selected group, selected leader
	sGroupIndex int
	sLeader     int
	// 全网随机selected leader
	nSLeader int
	// channel buffer for sending messages
	sendMsgCh chan *SendMessage

	stopCh chan struct{}

	wgReceivers sync.WaitGroup
}

func NewSuperNode(clientNo int) *SuperNode {
	groupCount := 65
	clientGroup := make([][]int, groupCount)
	for i := range clientGroup {
		clientGroup[i] = []int{}
	}

	streams := make([]*bufio.ReadWriter, clientNo)

	return &SuperNode{
		streams,
		clientNo, groupCount, clientGroup,
		nil,
		0, 0,
		0, // nSLeader
		nil,
		nil, // stopCh
		sync.WaitGroup{},
	}
}

func (node *SuperNode) clear() {
	clientGroup := make([][]int, node.groupCount)
	for i := range clientGroup {
		clientGroup[i] = []int{}
	}
	node.clientGroup = clientGroup
	node.sGroupIndex = 0
	node.sLeader = 0
	node.doneChan = make(chan int)

	node.nSLeader = rand.Int() % node.clientNo

	node.sendMsgCh = make(chan *SendMessage, 100)

	node.stopCh = make(chan struct{})
	// stopCh is an additional signal channel.
	// Its sender is the receiver of channel dataCh.
	// Its reveivers are the senders of channel dataCh.
}

var totalMessages atomic.Int64
var tttm int

func (node *SuperNode) sendMessage(msg *SendMessage) {
	if DEBUG {
		totalMessages.Add(1)
		tttm++
	}

	node.Streams[msg.m].WriteString(msg.s)
	node.Streams[msg.m].Flush()
}

// entry point
func (node *SuperNode) Process(times int) {
	var durations []time.Duration

	for i := 0; i < times; i++ {
		log.Println("Starting Epoch:", i+1)

		node.clear()
		node.makeClientGroup()

		if DEBUG {
			totalMessages.Store(0)
			tttm = 0
		}

		d, isTimeout := node.doRepeatProcess()
		if !isTimeout {
			durations = append(durations, d)
			log.Printf("Epoch (%d/%d): %v (ms)\n", i+1, times, d.Milliseconds())
			if DEBUG {
				log.Println("[DEBUG]totalMessages=", totalMessages.Load(), "tttm=", tttm)
			}
		} else {
			log.Printf("Epoch (%d/%d): %v (ms) [timeout]\n", i+1, times, d.Milliseconds())
		}

		node.wgReceivers.Wait()

		time.Sleep(2 * time.Second)
	}

	file, err := os.Create("records.csv")
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	defer file.Close()
	w := csv.NewWriter(file)
	defer w.Flush()

	var data [][]string
	for _, record := range durations {
		row := []string{fmt.Sprintf("%d", record.Milliseconds())}
		data = append(data, row)
	}
	w.WriteAll(data)
}

func (node *SuperNode) makeClientGroup() {
	flags := make([]bool, node.clientNo)
	q := node.clientNo / node.groupCount // at least q nodes in a group

	for i := 0; i < node.groupCount; i++ {
		for taken := 0; taken < q; {
			picked := rand.Intn(node.clientNo)
			if !flags[picked] {
				flags[picked] = true
				taken++
				node.clientGroup[i] = append(node.clientGroup[i], picked)
			}
		}
	}
	// deal with remaining {r} client
	for i, v := range flags {
		if !v {
			group := rand.Int() % node.groupCount
			node.clientGroup[group] = append(node.clientGroup[group], i)
		}
	}
}

func (node *SuperNode) doRepeatProcess() (time.Duration, bool) {
	// 1. start time measurement
	start := time.Now()
	// ... do experiment process
	node.sGroupIndex = rand.Int() % node.groupCount
	sGroup := node.clientGroup[node.sGroupIndex]
	sLeaderIndex := rand.Int() % len(sGroup)
	node.sLeader = sGroup[sLeaderIndex]
	// send to leader
	if DEBUG {
		log.Println("[DEBUG]send to leader", node.sLeader)
	}

	node.sendMessage(&SendMessage{m: node.sLeader, s: fmt.Sprintf("%d\n", message.LEADER)})

	// broadcast leader's block and other operations will be processed in handleStream
	// check for done condition
	var doneCount atomic.Int32
	isTimeout := false
loop:
	for { // listen for messages
		select {
		case msg := <-node.sendMsgCh:
			node.sendMessage(msg)
		case <-node.doneChan:
			doneCount.Add(1)

			// stop condition
			if DEBUG {
				log.Println("[DEBUG]done count: doneCount=", doneCount.Load())
			}
			if int(doneCount.Load()) == node.clientNo {
				if DEBUG {
					log.Println("[DEBUG]all done!!!")
				}
				close(node.stopCh)
				break loop
			}
			// case <-time.After(10 * time.Second):
			// 	// 设置超时，避免因丢包收不到足够的消息而卡住
			// 	if DEBUG {
			// 		log.Println("[ERROR]server timeout done!")
			// 	}
			// 	isTimeout = true
			// 	close(node.stopCh)
			// 	close(node.doneChan)
			// 	break loop
		}
	}

	// end time measurement
	return time.Since(start), isTimeout
}

func (node *SuperNode) ProcessStream(rw *bufio.ReadWriter) {
	for { // listen for messages
		str, _ := rw.ReadString('\n')
		if len(str) > 0 && str != "\n" {
			if DEBUG {
				log.Print("[DEBUG]received message:", str)
			}

			mType, _ := strconv.Atoi(str[:1])
			switch message.MessageType(mType) {
			case message.BLOCK:
				go node.recvBlock(str)
			case message.VOTE:
				go node.recvVote(str, node.clientNo)
			case message.DONE:
				go node.recvDone(str)
			case message.BLOCK0:
				go node.recvBlock0(str)
			case message.VOTE0:
				go node.recvVote(str, len(node.clientGroup[node.sGroupIndex]))
			case message.E:
				go node.recvE(str)
			case message.E1:
				go node.recvE1(str)
			}
		}
	}
}

func (node *SuperNode) doBroadcast(s string, debugInfo string) {
	for i := 0; i < len(node.clientGroup); i++ {
		g := node.clientGroup[i]
		for _, m := range g {
			node.wgReceivers.Add(1)
			go func(m int, vote string) {
				defer node.wgReceivers.Done()

				if DEBUG {
					log.Print(debugInfo, "type=", vote[:1], " msg:", vote)
				}

				//TODO: 给发送的节点记录票数
				select {
				case <-node.stopCh:
					return
				case node.sendMsgCh <- &SendMessage{m: m, s: vote}:
				}
			}(m, s)
		}
	}
}

// block0: "[TYPE]\n"
func (node *SuperNode) recvBlock0(str string) {
	// block0: "[TYPE][CREATE_VOTE][client]\n"
	t, _ := strconv.Atoi(str[:1])
	node.sendBlock2SGroup(str, message.MessageType(t))
}

// block: "[TYPE]\n"
func (node *SuperNode) recvBlock(str string) {
	// block: "[TYPE][CREATE_VOTE][client]\n"
	t, _ := strconv.Atoi(str[:1])
	node.sendBlock2All(str, message.MessageType(t))
}

func (node *SuperNode) sendBlock2SGroup(str string, mType message.MessageType) {
	// 1. send block to sGroup members

	sGroup := node.clientGroup[node.sGroupIndex]
	for _, m := range sGroup {
		node.wgReceivers.Add(1)
		go func(m int) {
			defer node.wgReceivers.Done()

			// block: "[TYPE][CREATE_VOTE][client]\n"
			s1 := fmt.Sprintf("%d%d%05d\n", mType, 1, m)
			if DEBUG {
				log.Println("[DEBUG]send block msg to sGroup:", s1)
			}

			select {
			case <-node.stopCh:
				return
			case node.sendMsgCh <- &SendMessage{m: m, s: s1}:
			}
		}(m)
	}
}

func (node *SuperNode) sendBlock2All(str string, mType message.MessageType) {
	// 1. send block to sGroup members
	node.sendBlock2SGroup(str, mType)

	// 2. send block to all other clients
	for i := 0; i < len(node.clientGroup); i++ {
		if i != node.sGroupIndex {
			g := node.clientGroup[i]
			for _, m := range g {
				node.wgReceivers.Add(1)
				go func(m int) {
					defer node.wgReceivers.Done()

					// block: "[TYPE][CREATE_VOTE][client]\n"
					s := fmt.Sprintf("%d%d%05d\n", mType, 1, m)
					if DEBUG {
						log.Println("[DEBUG]send block msg to others:", s)
					}

					select {
					case <-node.stopCh:
						return
					case node.sendMsgCh <- &SendMessage{m: m, s: s}:
					}
				}(m)
			}
		}
	}
}

// vote: "[TYPE][client]\n"
func (node *SuperNode) recvVote(str string, total int) {
	// broadcast vote to all clients
	index := len(str) - 1
	// vote: "[TYPE][client][total]\n"
	vote := fmt.Sprintf("%s%d%s", str[:index], total, str[index:])
	node.doBroadcast(vote, "[DEBUG]send vote msg:")
}

// E: "[TYPE=E][vote_client][nodeE_client]\n"
func (node *SuperNode) recvE(str string) {
	// E: [TYPE=E][vote_client][nodeE_client][N_client_total]\n
	s := fmt.Sprintf("%s%05d\n", str[:len(str)-1], node.clientNo)
	node.doBroadcast(s, "[DEBUG]send E msg:")
}

// E1: "[TYPE=E1][vote_client][nodeE1_client]\n"
func (node *SuperNode) recvE1(str string) {
	// broadcast E1: "[TYPE=E1][vote_client][nodeE1_client][N_sLeader][E1_total]\n"
	index := len(str) - 1
	s := fmt.Sprintf("%s%05d%d\n", str[:index], node.nSLeader, node.clientNo*len(node.clientGroup[node.sGroupIndex]))
	node.doBroadcast(s, "[DEBUG]send E1 msg:")
}

// done: "[TYPE][client]\n"
func (node *SuperNode) recvDone(str string) {
	i, _ := strconv.Atoi(str[1:5])
	node.doneChan <- i
}
