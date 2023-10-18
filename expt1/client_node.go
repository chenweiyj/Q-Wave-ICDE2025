package expt1

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"time"

	. "chain/util/const"
)

type ClientNode struct {
	rw     *bufio.ReadWriter
	client int
	block  string
	votes  map[int]struct{}
	total  int
	sendMsgDelay  time.Duration
}

func NewClientNode(rw *bufio.ReadWriter, delay int) *ClientNode {
	c := new(ClientNode)
	c.sendMsgDelay = time.Duration(delay) * time.Millisecond
	c.rw = rw
	c.votes = make(map[int]struct{})
	c.clear()
	return c
}

func (c *ClientNode) sendMessage(s string) {
	if c.sendMsgDelay > 0 {
		time.Sleep(c.sendMsgDelay) // 延迟200毫秒
	}

	c.rw.WriteString(s)
	c.rw.Flush()
}

func (c *ClientNode) clear() {
	c.client = -1
	c.block = ""
	c.total = -1
	for k := range c.votes {
		delete(c.votes, k)
	}
}

func (c *ClientNode) process() {
	msgCh := make(chan string)
	isDone := false

	go func(msgCh chan string) {
		for {
			select {
			case s := <-msgCh:
				c.recvMsg(s)

				// check number
				if len(c.votes) == c.total && c.block != "" {
					// send done to super node
					// done: "[TYPE][my_client]\n"
					s := fmt.Sprintf("%d%05d\n", 3, c.client)

					if DEBUG {
						log.Println("[DEBUG]send done message:", s, c.client)
					}

					c.sendMessage(s)

					isDone = true
					time.Sleep(time.Second)
					c.clear()
				} else {
					isDone = false
				}
			case <-time.After(3 * time.Second):
				// 设置超时，避免因丢包收不到足够的消息而卡住
				if !isDone {
					if DEBUG {
						log.Println("[ERROR]client timeout")
					}
					isDone = true
					c.clear()
				}
			}
		}

	}(msgCh)

	for { // listen for messages
		str, _ := c.rw.ReadString('\n')
		if len(str) > 0 && str != "\n" {
			// log.Println("[DEBUG]received message:", str)

			mType, _ := strconv.Atoi(str[:1])

			switch mType {
			case 0:
				go c.recvLeader()
			case 1:
				if DEBUG {
					log.Println("[DEBUG]received block0 message:", str)
				}
				msgCh <- str
			case 2:
				if DEBUG {
					log.Println("[DEBUG]received vote0 message:", str)
				}
				msgCh <- str
			}
		}
	}
}

func (c *ClientNode) recvMsg(msg string) {
	mType, _ := strconv.Atoi(msg[:1])
	switch mType {
	case 1:
		c.recvBlock(msg)
	case 2:
		c.recvVote(msg)
	}
}

// leader: "[TYPE]\n"
func (c *ClientNode) recvLeader() {
	// send block to super node
	s := "1\n"
	c.sendMessage(s)
}

// block: "[TYPE][CREATE_VOTE][client]\n"
func (c *ClientNode) recvBlock(str string) {
	if DEBUG {
		log.Println("[DEBUG]received block message:", str)
	}
	if c.block != "" {
		return
	}
	// 1. record the block
	c.block = str
	// 2. send vote to super node if necessary
	v := str[1:2]
	c.client, _ = strconv.Atoi(str[2 : len(str)-1])

	if DEBUG {
		log.Println("[DEBUG]client no:", c.client)
	}

	if v == "1" {
		// create vote: "[TYPE][client]\n"
		s := fmt.Sprintf("%d%s", 2, str[2:])
		c.sendMessage(s)
	}

	// close(ch)
}

// vote: "[TYPE][client][total]\n"
func (c *ClientNode) recvVote(str string) {
	vclient, _ := strconv.Atoi(str[1:6])
	total, _ := strconv.Atoi(str[6 : len(str)-1])
	c.votes[vclient] = struct{}{}
	c.total = total

	if DEBUG {
		log.Println("[DEBUG]received vote message:", str, "for client:", c.client)
	}
}
