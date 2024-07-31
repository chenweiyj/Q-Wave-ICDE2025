import socket
import threading
import pickle
import random
import time
import hashlib
import math
import queue
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from collections import deque
import base64
import argparse
CHUNK_SIZE = 1024 
RETRY_LIMIT = 15 
RETRY_DELAY = 2   
global_instance_list = []

def hash_content(content):
    return hashlib.sha256(content.encode()).hexdigest()
class Node:
    def __init__(self, node_id, address, coordinator_address):
        self.node_id = node_id
        self.address = address
        self.shard_addresses = []  # List of addresses within the same shard
        self.shard_id = None
        self.is_primary = False
        self.coordinator_address = coordinator_address
        self.state = defaultdict(set)
        self.view = 0
        self.f = 0
        self.pre_prepared = defaultdict(set)
        self.pre_prepared_lock = threading.Lock()
        self.prepared = defaultdict(set)
        self.prepared_lock = threading.Lock()
        self.committed = defaultdict(set)
        self.committed_lock = threading.Lock()
        self.received_messages = defaultdict(dict)
        self.received_messages_lock  = threading.Lock()
        self.broadcast_status = defaultdict(lambda: defaultdict(set))
        self.broadcast_status_lock  = threading.Lock()
        self.start_time = defaultdict(lambda: defaultdict(set))
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.message_queue = queue.Queue()
        self.received_message_queue = queue.Queue()
        self.messages_to_send=defaultdict(list)
        self.meesages_to_send_lock=threading.Lock()
        self.qualified = defaultdict(int)
        self.qualified_lock = threading.Lock()
        self.ack_list = []
        self.ack_list_lock=threading.Lock()
        self.retransit_sent =  defaultdict(list)
        self.retransit_sent_lock=threading.Lock()
        self.received = defaultdict(bool)
        self.received_lock = threading.Lock()
        self.finished = False
        self.finished_completed = False
        self.BB=False
        try:
            self.server_socket.bind(self.address)
            print(f"Successfully bound to {self.address}")
        except socket.error as e:
            print(f"Failed to bind to {self.address}: {e}")
            # Bind error occurred, let's try binding to a random port
            while True:
                # Generate a random port number
                random_port = random.randint(1024, 65535)
                try:
                    # Attempt to bind to the new port
                    self.server_socket.bind((self.address[0], random_port))
                    print(f"Successfully bound to {(self.address[0], random_port)}")
                    self.address= (self.address[0], random_port)
                    break  # Exit the loop if successful
                except socket.error as e:
                    print(f"Failed to bind to {(self.address[0], random_port)}: {e}")
                    continue  # Continue trying with another random port
        self.server_socket.listen(5)
        threading.Thread(target=self.listen_to_messages).start()
        threading.Thread(target=self.broadcasting_).start()
        threading.Thread(target=self.broadcasting_).start()
        threading.Thread(target=self.message_process).start()
        threading.Thread(target=self.await_broadcast).start()
        self.register_with_coordinator()
    def if_finished (self):
        return self.finished
    def finish (self):
        self.finished_completed=True
        self.server_socket.close()
    def register_with_coordinator(self):
        message = {
            'type': 'register',
            'node_id': self.node_id,
            'address': self.address
        }
        self.send_message(self.coordinator_address, message)
    def listen_to_messages(self):
        while not self.finished_completed:
            client_socket, _ = self.server_socket.accept()
            threading.Thread(target=self.handle_message, args=(client_socket,)).start()

    def handle_message(self,client_socket):
        data = b''
        client_socket.settimeout(3.0)  # Adjust timeout as necessary
        try:
            while not self.finished_completed:
                chunk = client_socket.recv(CHUNK_SIZE)
                if not chunk:
                    break
                data += chunk
                if b"*EOF" in data:
                    client_socket.sendall(b"pong")
                    break
        except Exception as e:
            return
        finally:
            client_socket.close()
        message_=pickle.loads(data.split(b"*EOF")[0])
        if isinstance(message_,dict):
            message_=[message_]
        if isinstance(message_,list):
            for message in message_:
                if message['type'] == 'node_addresses':
                        self.shard_addresses = message['addresses']
                        self.shard_id = message['shard_id']
                        self.BB = message['BB']
                        print (self.shard_addresses)
                        self.is_primary = (self.address == self.shard_addresses[0])  # First node in list is primary
                        temp= message.get('retransmit')
                        if len(global_instance_list)>0:
                            try:
                                for ii in global_instance_list:
                                    if ii.if_finished():
                                        ii.finish()
                                        global_instance_list.remove (ii)
                            except Exception as e:
                                print (f"error in delecting {e}")
                                pass
                        if temp!=None and temp >0:
                            self.retransmit(message)
                        else:
                            if self.is_primary and temp == None:
                                print ("I am primary")
                                message['retransmit']=2
                                self.direct_broadcast(message)
                            self.f =int((len(self.shard_addresses)-1)/int(message['f']))
                else:
                    if self.f!=0:
                        self.receive_message(message)
                    else:
                        self.received_message_queue.put(message)
    def message_process(self):
        while not self.finished_completed:
            if self.f!=0: #implying that the node address are received.
                message = self.received_message_queue.get()
                try:
                    self.receive_message(message)
                    if self.received_message_queue.empty():
                        break
                except Exception as e:
                    print(f"There is a error in {e}")
    def broadcasting_(self):
        while not self.finished_completed:
            try:
                address = self.message_queue.get()
                with self.meesages_to_send_lock:
                    temp=self.messages_to_send[address]
                    if len(temp) != 0:
                        self.messages_to_send[address]= []
                        self.send_message(address,temp)
            except Exception as e:
                print ("EError:",e)
    def send_message(self, address, message):
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(address)
            data = pickle.dumps(message)
            client_socket.sendall(data+b"*EOF")
            client_socket.settimeout(3.0)
            data=client_socket.recv(1024)
            client_socket.close()
            if data.decode('utf-8') != "pong":
                raise Exception ('no pong')
        except:
            with self.meesages_to_send_lock:
                if isinstance(message,list):
                    self.messages_to_send[address]+=message
                else:
                    self.messages_to_send[address].append(message)
                self.message_queue.put((address))
        #sender(address, message)
    def direct_broadcast(self, message):
        my_index = self.shard_addresses.index(self.address)
        sqrt_len = math.ceil(math.sqrt(len(self.shard_addresses)))
        with self.meesages_to_send_lock:
            for i in range (sqrt_len):
                next_index = (my_index +  (i) * sqrt_len ) % len(self.shard_addresses)
                self.messages_to_send[self.shard_addresses[next_index]].append(message)
                self.message_queue.put((self.shard_addresses[next_index]))
                
    def retransmit(self, message):
 #       # Get the index of this node in the shard_addresses
        if  message['retransmit']==0:
            return
        my_index = self.shard_addresses.index(self.address)
        sqrt_len = math.ceil(math.sqrt(math.sqrt(len(self.shard_addresses))))
        hash_content_=hash_content(str(message))
        if message['retransmit']==2:
            message['retransmit']=1
            with self.meesages_to_send_lock:
                if hash_content_ not in self.retransit_sent["2"]:
                    self.retransit_sent["2"].append(hash_content_)
                    for i in range (sqrt_len):
                        next_index = (my_index + (i) * sqrt_len) % len(self.shard_addresses)
                        self.messages_to_send[self.shard_addresses[next_index]].append(message)
                        self.message_queue.put((self.shard_addresses[next_index]))
         #   self.retransmit(message)
        elif message['retransmit']==1:
            message['retransmit']=0
            with self.meesages_to_send_lock:
                if hash_content_ not in self.retransit_sent["1"]:
                    self.retransit_sent["1"].append(hash_content_)
                    for i in range (sqrt_len):
                        next_index = (my_index + i) % len(self.shard_addresses)
                        self.messages_to_send[self.shard_addresses[next_index]].append(message)
                        self.message_queue.put((self.shard_addresses[next_index]))

    def receive_message(self, message):
        temp= message.get('retransmit')
        if temp!=None and temp >0:
            self.retransmit(message)
        else:
            if message['type'] == 'repeat_experiment':
                print(f"Node {self.node_id} received request to repeat experiment.")
                if message['ID'] not in self.start_time:
                    self.start_time[message['ID']][self.view]=message['start_time']
                    self.state[message['ID']] = 'initial'
                if self.is_primary and temp == None:
                    print ("I am primary")
                    message['retransmit']=2
                    self.direct_broadcast(message)
                    self.start_protocol("Sample Request" * 1024 * 5,message['ID'])
            elif message['type'] == 'start_new_experiment':
                print(f"Node {self.node_id} received request to start new experiment.")
                if self.is_primary and temp == None:
                    print ("I am primary")
                    message['retransmit']=2
                    self.direct_broadcast(message)
                if temp ==0 and  self.finished == False:
                    self.finished = True
                    self.activate_a_new_instance(self.node_id, self.address, self.coordinator_address)
            else:
                message_type = message['type']
                if message_type == 'ack-votes':
                    if hash_content(str(message)) not in self.received:
                        with self.received_lock:
                            self.received[hash_content(str(message))]=True
                        for tt in message['message_id']:
                            with self.qualified_lock:
                                if tt not in self.qualified:
                                    self.qualified[tt] = 1
                                else:
                                    self.qualified[tt]+=1
                            if self.qualified[tt]>self.f and tt in self.received_messages:
                                with self.qualified_lock:
                                    self.qualified[tt]=-5000
                                self.handle_qualified(self.received_messages[tt])
                else:
                    if self.finished ==True:
                        return
                    if self.BB==False:
                        self.handle_qualified(message)
                    else:
                        message_id = (hash_content(str(message)))
                        if message_id not in self.received_messages:
                            with self.received_messages_lock:
                                self.received_messages[message_id]= message
                            with self.ack_list_lock:
                                self.ack_list.append(message_id)
                            if self.qualified[message_id]>self.f:
                                self.handle_qualified(message)
                                with self.qualified_lock:
                                    self.qualified[message_id]=-5000 #threading.Thread(target=self.handle_qualified, args=(message,)).start()
    def await_broadcast (self):
        while not self.finished_completed:
            time.sleep(5)
            with self.ack_list_lock:
                if len(self.ack_list)!=0:
                    message={'message_id':self.ack_list, 'type':"ack-votes", 'sender':self.node_id, 'view':self.view, 'retransmit':2}
                    self.direct_broadcast(message)
                    self.ack_list= []
    def handle_qualified (self, message):
                message_type = message['type']
                view = message['view']
                seq_num = message['seq_num']
                content = message.get('content')
                content_hash = message.get('content_hash')
                if content_hash is None and content is not None:
                    content_hash = hash_content(content)
                sender = message['sender']
                message_id = (message_type, view, seq_num, content_hash)
                message_type = message['type']
                if message_type == 'pre-prepare':
                    if view == self.view and self.valid_message(message):
                        if message_id not in self.pre_prepared[seq_num]:
                            with self.pre_prepared_lock:
                                self.pre_prepared[seq_num].add(message_id)
                            if 'prepare' not in self.broadcast_status[seq_num][view]:
                                with self.broadcast_status_lock:
                                    self.broadcast_status[seq_num][view].add('prepare')
                                self.direct_broadcast({
                                    'type': 'prepare',
                                    'view': view,
                                    'seq_num': seq_num,
                                    'content_hash': content_hash,
                                    'sender': self.node_id,
                                    'retransmit' : 2
                                })
                elif message_type == 'prepare':
                    if view == self.view and self.valid_message(message):
                        prepare_id = (sender, seq_num, content_hash)
                        if prepare_id not in self.prepared[seq_num]:
                            with self.prepared_lock:
                                self.prepared[seq_num].add(prepare_id)
                            if len(self.prepared[seq_num]) >len(self.shard_addresses)-self.f and 'commit' not in self.broadcast_status[seq_num][view]:
                                with self.broadcast_status_lock:
                                    self.broadcast_status[seq_num][view].add('commit')
                                self.direct_broadcast({
                                    'type': 'commit',
                                    'view': view,
                                    'seq_num': seq_num,
                                    'content_hash': content_hash,
                                    'sender': self.node_id,
                                    'retransmit' : 2
                                })
                elif message_type == 'commit':
                    if view == self.view and self.valid_message(message):
                        commit_id = (sender, seq_num, content_hash)
                        if commit_id not in self.committed[seq_num]:
                            with self.committed_lock:
                                self.committed[seq_num].add(commit_id)
                            if len(self.committed[seq_num]) > len(self.shard_addresses)-self.f and 'terminate' not in self.broadcast_status[seq_num][view]:
                                with self.broadcast_status_lock:
                                    self.broadcast_status[seq_num][view].add('terminate')
                if 'prepare' in self.broadcast_status[seq_num][view] and 'commit' in self.broadcast_status[seq_num][view] and 'terminate' in self.broadcast_status[seq_num][view]:
                        self.execute(seq_num)
    def valid_message(self, message):
        required_keys = {'type', 'view', 'seq_num', 'sender'}
        return all(key in message for key in required_keys)
    def execute(self, seq_num):
        with self.broadcast_status_lock:
            if self.state[seq_num] == 'initial':
                self.state[seq_num] = 'executed'
                finish_time = time.time()
                print(f"Node {self.node_id} finished execution at {finish_time}")
                self.send_finished_message(finish_time,seq_num)
    def send_finished_message(self, finish_time,seq_num):
        message = {
            'type': 'finished',
            'seq_num':seq_num,
            'node_id': self.node_id,
            'address': self.address,
            'finish_time': finish_time - self.start_time[seq_num][self.view]
        }
        self.send_message(self.coordinator_address, message)
        print ("I sent the final signal to coordinator")
    def start_protocol(self, content,ID):
        print(f"Protocol started at { time.time()}")
        seq_num = ID
        content_hash = hash_content(content)
        self.state[seq_num] = 'initial'  # Reset state
        self.direct_broadcast({
            'type': 'pre-prepare',
            'view': self.view,
            'seq_num': seq_num,
            'content': content,
            'content_hash': content_hash,
            'sender': self.node_id,
            'retransmit' : 2
        })
    def activate_a_new_instance (self,node_id, address, coordinator_address):
        global global_instance_list
        global_instance_list.append(Node(node_id, address, coordinator_address))
        print("started a new run")
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a PBFT node.")
    parser.add_argument("--node_id", type=int, required=True, help="Node ID")
    parser.add_argument("--host", type=str, required=True, help="Host address")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    parser.add_argument("--coordinator_address", type=str, required=True, help="Coordinator node address")
    args = parser.parse_args()
    node_id = args.node_id
    address = (args.host, args.port)
    coordinator_address = eval(args.coordinator_address)
    global_instance_list.append(Node(node_id, address, coordinator_address))
    print("started")