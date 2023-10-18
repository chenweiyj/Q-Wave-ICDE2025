package expt2

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	. "chain/util/const"
	message "chain/util/message"
)

type ClientNode struct {
	rw     *bufio.ReadWriter
	client int
	block  string
	isReceivedBlock0 atomic.Bool
	isReceivedBlock atomic.Bool
	votes  map[int]struct{}
	total  int
	// E message count for each vote
	eMessageCountForVote    map[int]int
	eMessageForVoteFinished map[int]bool
	e1MessageCount          atomic.Int32
}

func NewClientNode(rw *bufio.ReadWriter, delay int, client int) *ClientNode {
	c := new(ClientNode)
	c.rw = rw
	c.votes = make(map[int]struct{})
	c.client = client
	c.clear()
	return c
}

func (c *ClientNode) sendMessage(s string, mType message.MessageType) {
	// if c.sendMsgDelay > 0 {
	// 	time.Sleep(c.sendMsgDelay) // 延迟200毫秒
	// }

	if DEBUG {
		log.Printf("[DEBUG]client %d send %s msg: %s\n", c.client, mType.String(), s)
	}

	c.rw.WriteString(s)
	c.rw.Flush()
}

func (c *ClientNode) clear() {
	c.block = ""
	c.isReceivedBlock0.Store(false)
	c.isReceivedBlock.Store(false)
	c.total = -1
	for k := range c.votes {
		delete(c.votes, k)
	}
	c.eMessageCountForVote = make(map[int]int)
	c.eMessageForVoteFinished = make(map[int]bool)
}

func (c *ClientNode) Process() {
	msgCh := make(chan string)
	var isDone atomic.Bool
	isDone.Store(true)

	go func(msgCh chan string) {
		for {
			select {
			case s := <-msgCh:
				c.recvMsg(s)

				// check number
				if DEBUG {
					log.Println("[DEBUG]check done condition for client:", c.client, "isDone=", isDone.Load(), "votesCount=", len(c.votes), "totalVotes=", c.total, "isBlockEmpty=", !c.isReceivedBlock.Load())
				}
				if !isDone.Load() && len(c.votes) >= 2*c.total/3 && c.block != "" {
					// send done to super node
					// done: "[TYPE][my_client]\n"
					s := fmt.Sprintf("%d%05d\n", message.DONE, c.client)

					c.sendMessage(s, message.DONE)

					isDone.Store(true)
					c.clear()
					time.Sleep(time.Second)
				} else {
					isDone.Store(false)
				}
			case <-time.After(10 * time.Second):
				// 设置超时，避免因丢包收不到足够的消息而卡住
				if !isDone.Load() {
					if DEBUG {
						log.Println("[ERROR]client timeout")
					}
					isDone.Store(true)
					c.clear()
				}
			}
		}

	}(msgCh)

	for { // listen for messages
		str, _ := c.rw.ReadString('\n')
		if len(str) > 0 && str != "\n" {
			msgCh <- str
		}
	}
}

func (c *ClientNode) recvMsg(msg string) {
	mType, _ := strconv.Atoi(msg[:1])
	switch message.MessageType(mType) {
	case message.LEADER:
		c.recvLeader()
	case message.BLOCK:
		c.recvBlock(msg, message.BLOCK)
	case message.VOTE:
		c.recvVote(msg)
	case message.BLOCK0:
		c.recvBlock(msg, message.BLOCK0)
	case message.VOTE0:
		c.recvVote0(msg)
	case message.E:
		c.recvE(msg)
	case message.E1:
		c.recvE1(msg)
	}
}

// leader: "[TYPE]\n"
func (c *ClientNode) recvLeader() {
	// send block to super node
	// block0: [TYPE]\n
	s := fmt.Sprintf("%d\n", message.BLOCK0)

	c.sendMessage(s, message.BLOCK0)
}

// block: "[TYPE][CREATE_VOTE][client]\n"
func (c *ClientNode) recvBlock(str string, mType message.MessageType) {
	if DEBUG {
		log.Printf("[DEBUG]received block message: %s, mType=%d\n", str, mType)
	}

	var sendType message.MessageType

	// 1. record the block
	if mType == message.BLOCK0 {
		if DEBUG {
			log.Printf("[DEBUG]check block message of type=%d: c.block=%v\n", mType, c.isReceivedBlock0.Load())
		}
		if c.isReceivedBlock0.Load() {
			return
		}
		c.isReceivedBlock0.Store(true)
		sendType = message.VOTE0
		// c.block = str
	} else if mType == message.BLOCK {
		if DEBUG {
			log.Printf("[DEBUG]check block message of type=%d: c.block=%v\n", mType, c.isReceivedBlock.Load())
		}
		if c.isReceivedBlock.Load() {
			return
		}
		c.isReceivedBlock.Store(true)
		c.block = str
		sendType = message.VOTE
	} else {
		return
	}

	// 2. send vote to super node if necessary
	v := str[1:2]
	// c.client, _ = strconv.Atoi(str[2 : len(str)-1])

	// if DEBUG {
	// 	log.Println("[DEBUG]client no:", c.client)
	// }

	if v == "1" {
		// create vote: "[TYPE=VOTE][client]\n"
		s := fmt.Sprintf("%d%s", sendType, str[2:])
		c.sendMessage(s, sendType)
	}
}

// vote: "[TYPE=VOTE0][vote_client][total=len(sGroup)]\n"
func (c *ClientNode) recvVote0(str string) {
	if DEBUG {
		log.Printf("[DEBUG]received %s message: %s for client: %d votes count: %d total: %d\n", message.VOTE0.String(), str, c.client, len(c.votes), c.total)
	}

	// E: [TYPE=E][vote_client][nodeE_client]\n
	s := fmt.Sprintf("%d%s%05d\n", message.E, str[1:6], c.client)
	c.sendMessage(s, message.E)
}

// vote: "[TYPE][client][total]\n"
func (c *ClientNode) recvVote(str string) {
	vclient, _ := strconv.Atoi(str[1:6])
	total, _ := strconv.Atoi(str[6 : len(str)-1])
	c.votes[vclient] = struct{}{}
	c.total = total

	if DEBUG {
		log.Println("[DEBUG]received vote message:", str, "for client:", c.client, "votes count:", len(c.votes), "total:", c.total)
	}
}

// E: [TYPE=E][vote_client][nodeE_client][N_client_total]\n
func (c *ClientNode) recvE(str string) {
	// wait for 2/3*N E message
	voteClient, _ := strconv.Atoi(str[1:6])
	nClientTotal, _ := strconv.Atoi(str[len(str)-6 : len(str)-1])

	c.eMessageCountForVote[voteClient]++
	voteCount := c.eMessageCountForVote[voteClient]
	if DEBUG {
		log.Printf("[DEBUG]received %s message: %s for client: %d votes count: %d total: %d\n", message.E.String(), str, c.client, voteCount, nClientTotal)
	}

	if voteCount >= 2*nClientTotal/3 && !c.eMessageForVoteFinished[voteClient] {
		c.eMessageForVoteFinished[voteClient] = true
		// send E1 message
		// E1: [TYPE=E1][vote_client][nodeE1_client]\n
		s := fmt.Sprintf("%d%05d%05d\n", message.E1, voteClient, c.client)
		c.sendMessage(s, message.E1)
	}
}
// 8 01346 01449 00481 34500
// 8 01040 00255 01007 22800
// E1: "[TYPE=E1][vote_client][nodeE1_client][N_sLeader][E1_total]\n"
func (c *ClientNode) recvE1(str string) {
	nSLeader, _ := strconv.Atoi(str[11:16])
	e1Total, _ := strconv.Atoi(str[16:len(str)-1])
	if nSLeader == c.client {
		c.e1MessageCount.Add(1)
		if DEBUG {
			log.Println("[DEBUG]N_sLeader E1 message count:", c.e1MessageCount.Load(), " total:", e1Total)
		}
		if e1Total == int(c.e1MessageCount.Load()) {
			// BLOCK: "[TYPE]\n"
			s := fmt.Sprintf("%d\n", message.BLOCK)

			c.sendMessage(s, message.BLOCK)
		}
	}
}
