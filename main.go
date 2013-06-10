// nmq project main.go
// nmq - Notification Message Queue
// by Rdev v.00.01 a

package main

import (
	"bufio"
	//   "bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"

//	"os"
)

// message struct
type Message struct {
	priority     int
	message_type int //message type 0-info, 1-notice, 2-warning, 3-err, 4-push
	body         []byte
}

func (msg *Message) Bytes() []byte {
	return msg.body
}

type MessageQueue struct {
	messages []*Message
	used     int
	name     string
	msg_type int
}

// get size of MQ
func (mq *MessageQueue) Len() int {
	return mq.used
}

//funnction to check priority between 2 msg
func (mq *MessageQueue) Less(i, j int) bool {
	return mq.messages[i].priority < mq.messages[j].priority
}

//function to swap to msg after Less
func (mq *MessageQueue) Swap(i, j int) {
	mq.messages[i], mq.messages[j] = mq.messages[j], mq.messages[i]
}

func (m *Message) Size() int {
	return (binary.Size(m.priority) + binary.Size(m.message_type) + binary.Size(m.body))
}

// Push to queue
func (mq *MessageQueue) Push(x interface{}) {
	if mq.used == len(mq.messages) {
		newSlice := make([]*Message, (1+len(mq.messages))*2)
		for i, c := range mq.messages {
			newSlice[i] = c
		}
		mq.messages = newSlice
	}
	mq.messages[mq.used] = x.(*Message)
	mq.used += 1
}

// Pop from queue
func (mq *MessageQueue) Pop() interface{} {
	if mq.used == 0 {
		return nil
	}
	m := mq.messages[mq.used-1]
	mq.messages = mq.messages[0 : mq.used-1]
	mq.used -= 1
	return m
}

func NewMessageQueue(size int, t_type int, t_name string) *MessageQueue {
	return &MessageQueue{messages: make([]*Message, size), used: 0, msg_type: t_type, name: t_name}
}

func (mq *MessageQueue) Give(m *Message) {
	log.Println("PUSH")
	heap.Push(mq, m)
}

func (mq *MessageQueue) Take() *Message {
	log.Println("POP")
	return heap.Pop(mq).(*Message)
}

// array of bytes to string
func bts(bytes []byte) string {
	return string(bytes)
}

// string to array of bytes
func stb(str string) []byte {
	return []byte(str)
}

//server implementation part

type Server struct {
	address string
	port    int
	timeout int
	//mq               *MessageQueue
	log              *log.Logger
	connections      int // count connected clients
	totalConnections int // total connected
	maxMemory        int
}

//var mq *MessageQueue = NewMessageQueue(1, 1, "3")
var mq []*Message = make([]*Message, 0)
var mq_bytes int = 0
var mq_count int = 0

func (s *Server) Init(address string, port int) net.Listener {
	listner, err := net.Listen("tcp", fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		log.Panic(err)
	}
	return listner
}

func server_connections(listener net.Listener) chan net.Conn {
	ch := make(chan net.Conn)
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				log.Printf("Failed to connect to %s\n", err)
				continue
			}
			log.Printf("Connected to %v\n", client.RemoteAddr())
			ch <- client
		}
	}()
	log.Printf("GOGOOG")
	return ch
}

func handle(client net.Conn) {
	buf := bufio.NewReader(client)
	log.Printf("NEW GORUTINE")
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			break
		}
		chunks := strings.Split(strings.Replace(string(line), "\n", "", -1), " ")
		if len(chunks) >= 1 {
			switch chunks[0] {
			case "put":
				tmp_item := new(Message)
				tmp_item.priority = 1
				tmp_item.message_type = 1
				tmp_item.body = []byte(strings.Replace(strings.Replace(string(line), "put ", "", 1), "\n", "", -1))
				mq = append(mq, tmp_item)
				tmp_s := tmp_item.Size()
				mq_bytes = mq_bytes + tmp_s
				mq_count++
				log.Printf("Stored new messages. Size: %d", tmp_s)
			case "get":
				if mq_count > 0 {
					tmp_item := new(Message)
					tmp_item = mq[0]
					mq = mq[1:]
					mq_bytes = mq_bytes - tmp_item.Size()
					mq_count--
					client.Write(tmp_item.body)
					client.Write([]byte("\n"))
				} else {
					client.Write([]byte("Message queue is empty!"))
					client.Write([]byte("\n"))
				}
			case "info":
				client.Write([]byte("INFO"))

			}
		}
	}

}

func (s *Server) Start(interf net.Listener) {
	connections := server_connections(interf)
	for {
		go handle(<-connections)
	}
}

func main() {
	runtime.GOMAXPROCS(4)
	log.Println("::.. NotificationMessageQueue v.0.1a by RDev ")
	srv := new(Server)
	inter := srv.Init("", 11111)
	srv.Start(inter)

	m := NewMessageQueue(1, 1, "3")
	a := Message{1, 1, []byte("Test1")}
	b := Message{2, 1, []byte("Test2")}
	c := Message{1, 1, []byte("Test3")}
	d := Message{1, 1, []byte("Test4")}

	m.Give(&a)
	m.Give(&b)
	m.Give(&c)
	m.Give(&d)

	tmp := m.Take()
	fmt.Println(bts(stb(bts(tmp.body))))
	tmp = m.Take()
	fmt.Println(bts(stb(bts(tmp.body))))
	tmp = m.Take()
	fmt.Println(bts(stb(bts(tmp.body))))
	tmp = m.Take()
	fmt.Println(bts(stb(bts(tmp.body))))

	fmt.Println("Hello World!")
}
