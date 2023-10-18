package expt1

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	. "chain/util/const"
)

type SendMessage struct {
	m int
	s string
}

type SuperNode struct {
	streams     []*bufio.ReadWriter
	clientNo    int
	groupCount  int
	clientGroup [][]int
	doneChan    chan int
	// selected group, selected leader
	sGroupIndex int
	sLeader     int
	// channel buffer for sending messages
	sendMsgCh chan *SendMessage
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
		nil,
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

	node.sendMsgCh = make(chan *SendMessage, 200)
}

func (node *SuperNode) sendMessage(msg *SendMessage) {
	node.streams[msg.m].WriteString(msg.s)
	node.streams[msg.m].Flush()
}

// entry point
func (node *SuperNode) process(times int) {
	var durations []time.Duration
	for i := 0; i < times; i++ {
		node.clear()
		node.makeClientGroup()

		d, isTimeout := node.doRepeatProcess()
		if !isTimeout {
			durations = append(durations, d)
			log.Printf("Epoch (%d/%d): %vms\n", i+1, times, d.Milliseconds())
		} else {
			log.Printf("Epoch (%d/%d): %vms [timeout]\n", i+1, times, d.Milliseconds())
		}

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
	rw := node.streams[node.sLeader]

	if DEBUG {
		log.Println("[DEBUG]send to leader", node.sLeader, rw)
	}

	rw.WriteString("0\n")
	rw.Flush()

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
				break loop
			}
		case <-time.After(3 * time.Second):
			// 设置超时，避免因丢包收不到足够的消息而卡住
			if DEBUG {
				log.Println("[ERROR]server timeout done!")
			}
			isTimeout = true
			close(node.doneChan)
			break loop
		}
	}

	// end time measurement
	return time.Since(start), isTimeout
}

func (node *SuperNode) processStream(rw *bufio.ReadWriter) {
	for { // listen for messages
		str, _ := rw.ReadString('\n')
		if len(str) > 0 && str != "\n" {
			if DEBUG {
				log.Println("[DEBUG]received message:", str)
			}

			mType, _ := strconv.Atoi(str[:1])

			switch mType {
			case 1:
				go node.recvBlock(str)
			case 2:
				go node.recvVote(str)
			case 3:
				go node.recvDone(str)
			}
		}
	}
}

// block: "[TYPE]\n"
func (node *SuperNode) recvBlock(str string) {
	// block: "[TYPE][CREATE_VOTE][client]\n"
	index := len(str) - 1
	// 1. send block to sGroup members

	sGroup := node.clientGroup[node.sGroupIndex]
	for _, m := range sGroup {
		go func(m int) {
			s1 := fmt.Sprintf("%s%d%05d%s", str[:index], 1, m, str[index:])
			if DEBUG {
				log.Println("[DEBUG]send block msg to sGroup:", s1)
			}

			node.sendMsgCh <- &SendMessage{m: m, s: s1}
		}(m)
	}

	// 2. send block to all other clients
	for i := 0; i < len(node.clientGroup); i++ {
		if i != node.sGroupIndex {
			g := node.clientGroup[i]
			for _, m := range g {
				go func(m int) {
					s0 := fmt.Sprintf("%s%d%05d%s", str[:index], 0, m, str[index:])
					if DEBUG {
						log.Println("[DEBUG]send block msg to others:", s0)
					}

					node.sendMsgCh <- &SendMessage{m: m, s: s0}
				}(m)
			}
		}
	}
}

// vote: "[TYPE][client]\n"
func (node *SuperNode) recvVote(str string) {
	// broadcast vote to all clients
	index := len(str) - 1
	// vote: "[TYPE][client][total]\n"
	vote := fmt.Sprintf("%s%d%s", str[:index], len(node.clientGroup[node.sGroupIndex]), str[index:])
	for i := 0; i < len(node.clientGroup); i++ {
		g := node.clientGroup[i]
		for _, m := range g {
			go func(m int, vote string) {
				if DEBUG {
					log.Println("[DEBUG]send vote msg:", vote)
				}

				//TODO: 给发送的节点记录票数

				node.sendMsgCh <- &SendMessage{m: m, s: vote}
			}(m, vote)
		}
	}
}

// done: "[TYPE][client]\n"
func (node *SuperNode) recvDone(str string) {
	i, _ := strconv.Atoi(str[1:5])
	node.doneChan <- i
}
