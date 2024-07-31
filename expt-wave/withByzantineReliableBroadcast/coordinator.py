import socket
import time
import threading
import pickle
import queue
import argparse
import random
import psutil
import sys
import os
import signal

from collections import defaultdict
from flask import Flask, jsonify, render_template, request
CHUNK_SIZE = 1024
app = Flask(__name__)
logs = []
logs_simplified = []
logs_lock=threading.Lock()
Shards_=[]
f_=[]
BB_=[]
class Coordinator:
    def __init__(self, address, total_nodes, f, BB, num_shards, repeat_times):
        self.address = address
        self.total_nodes = total_nodes
        self.num_shards = num_shards
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(self.address)
        self.server_socket.listen(14)
        self.shards = defaultdict(list)
        self.repeat_times = 0
        self.shard_leaders =[]
        self.node_addresses = []
        self.node_address_lock = threading.Lock()
        self.finished_times = []
        self.finished_times_lock = threading.Lock()
        self.finished_ = defaultdict(set)
        self.finished_lock = threading.Lock()
        self.messages_to_send=defaultdict(list)
        self.meesages_to_send_lock= threading.Lock()
        self.repeat_times = 0
        self.rpt= repeat_times
        self.message_queue = queue.Queue()
        self.broadcast_queue =queue.Queue()
        self.f = f
        self.BB= BB
        self.closed=False
        self.finish_register=False
        threading.Thread(target=self.listen_to_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.process_messages).start()
        threading.Thread(target=self.broadcasting_).start()
        threading.Thread(target=self.broadcasting_).start()
    def listen_to_messages(self):
        try:
            while not self.closed:
                client_socket, _ = self.server_socket.accept()
                threading.Thread(target=self.process_data,args=(client_socket,)).start()
        except Exception as e:
            print("error!!!!!!!!!!!!",e)
    def process_data(self, client_socket):
        try:
            data = b''
            client_socket.settimeout(3.0)  # Adjust timeout as necessary
            while True:
                chunk = client_socket.recv(CHUNK_SIZE)
                if not chunk:  # No more data, client closed connection
                    break
                data += chunk
                if b"*EOF" in data:  # Check for EOF marker
                    client_socket.sendall(b"pong")  # Acknowledge EOF
                    data = data.split(b"*EOF")[0]  # Remove EOF marker and extra data if any
                    message = pickle.loads(data)  # Deserialize data
                    if isinstance(message,list):
                        for u in message:
                            self.message_queue.put((u, ))
                    else:
                        self.message_queue.put((message, ))  # Enqueue message
                    break  # Exit the loop after handling EOF
        except socket.timeout:
            print("Socket timed out")
        except pickle.PickleError:
            print("Failed to deserialize data")
        except Exception as e:
            print("Error:", e)
        finally:
            client_socket.close()  # Ensure socket is closed
        
    def assign_nodes_to_shards(self):
        shard_size = self.total_nodes // self.num_shards
        extra_nodes = self.total_nodes % self.num_shards
        start_index = 0
        for i in range(self.num_shards):
            end_index = start_index + shard_size + (1 if i < extra_nodes else 0)
            self.shards[i] = self.node_addresses[start_index:end_index]
            start_index = end_index

    def process_messages(self):
        while True:
            try:
                message,  = self.message_queue.get()
                if self.finish_register and message['address'] not in self.node_addresses:
                    continue
                if message['type'] == 'register':
                  #  mes_ = {
                  #      'type': 'ack',
                  #      'seq_num': 'register'
                  #  }
                    if len(self.node_addresses) == self.total_nodes:
                        continue
                   # with self.meesages_to_send_lock:
                   #     if message['address'] in self.messages_to_send:
                   #         self.messages_to_send[message['address']].append(mes_)
                   #     else:
                   #         self.messages_to_send[message['address']]= [mes_]
                   #     self.broadcast_queue.put((message['address']))
                    if message['address'] in self.node_addresses:
                        continue
                    with self.node_address_lock:
                        self.node_addresses.append(message['address'])
                        log_message = f"Node {message['node_id']} registered with address {message['address']}"
                        print(log_message)
                        with logs_lock:
                            logs.append(log_message)
                            logs_simplified.append(log_message)
                        if len(self.node_addresses) == self.total_nodes:
                            self.node_addresses=random.sample(self.node_addresses, len(self.node_addresses))
                            threading.Thread(target=self.broadcast_node_addresses).start()
                            self.finish_register =True
                            print ("start to broadcast")
                elif message['type'] == 'finished':
                    #mes_ = {
                    #    'type': 'ack',
                    #    'seq_num': message['seq_num']
                    #}
                    #print ("finished message", message)
                    #with self.meesages_to_send_lock:
                    #    if message['address'] in self.messages_to_send:
                    #        self.messages_to_send[message['address']].append(mes_)
                    #    else:
                    #        self.messages_to_send[message['address']]= [mes_]
                    #    self.broadcast_queue.put((message['address']))
                    with self.finished_lock:
                        if message['node_id'] in self.finished_[message['seq_num']]:
                            continue
                    self.finished_[message['seq_num']].add(message['node_id'])
                    log_message = f"Node {message['node_id']} finished at {message['finish_time']}"
                    print(log_message)
                    with logs_lock:
                        logs.append(log_message)
                    with self.finished_times_lock:
                        self.finished_times.append(message['finish_time'])
                    if len(self.finished_times) == self.total_nodes:
                        last_finish_time = max(self.finished_times)
                        log_message = f"{self.repeat_times} The last node finished at {last_finish_time}"
                        print(log_message)
                        with logs_lock:
                            logs.append(log_message)
                            logs.append(self.repeat_times)
                            logs_simplified.append(log_message)
                            logs_simplified.append(self.repeat_times)
                        if self.repeat_times<self.rpt:
                            self.repeat_times+=1
                            with logs_lock:
                                log_message = "Repeat request sent"
                                logs.append(log_message)
                            threading.Thread(target=self.request_repeat_experiment, args=(self.repeat_times,)).start()
                            with logs_lock:
                                log_message = "Repeat request done"
                                logs.append(log_message)
                        else:
                            self.request_start_a_new_experiment()
            except Exception as e: 
                log_message = f"Something was wrong with the thread {e}, {message}"
                with logs_lock:
                    logs.append(log_message)
                print (log_message)
            finally:
                self.message_queue.task_done()

    def broadcast_node_addresses(self):
        with logs_lock:
            log_message = "Broadcast address started"
            logs.append(log_message)        
        self.assign_nodes_to_shards()
        for shard_id, addresses in self.shards.items():
            message = {
                'type': 'node_addresses',
                'addresses': addresses,
                'shard_id': shard_id,
                'f':self.f,
                'BB':self.BB
            }
            self.shard_leaders.append(addresses[0])
            self.send_message(addresses[0], message)
        log_message = "Broadcast address finished"
        with logs_lock:
            logs.append(log_message)
        self.request_repeat_experiment(0)
        log_message = "Experiment requested"
        with logs_lock:
            logs.append(log_message)

    def send_message(self, address, message):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(address)
        data = pickle.dumps(message)
        client_socket.sendall(data+b"*EOF")
        client_socket.settimeout(3.0)
        try:
            data=client_socket.recv(1024)
            client_socket.close()
            if data.decode('utf-8') != "pong":
                raise Exception ('no pong')
        except:
            with self.meesages_to_send_lock:
                if address in self.messages_to_send:
                    self.messages_to_send[address].append(message)
                else:
                    self.messages_to_send[address]= [message]
                self.broadcast_queue.put((address))
    
    def broadcasting_(self):
        while True:
            try:
                address = self.broadcast_queue.get()
                with self.meesages_to_send_lock:
                    temp=self.messages_to_send[address]
                    if temp != []:
                        self.messages_to_send[address]= []
                if temp != []:
                    self.send_message(address,temp)
            except Exception as e:
                if self.finished:
                    break
                print ("EError:",e)
    
    def request_repeat_experiment(self,ID):
        time.sleep(20)
        message = {
            'type': 'repeat_experiment',
            'start_time' : time.time(),
            'ID':ID
        }
        self.finished_times=[]
        self.finished_= defaultdict(set)
        for address in self.shard_leaders:
            self.send_message(address, message)

    def request_start_a_new_experiment(self):
        count_=0
        while (not self.message_queue.empty()) or  (not self.broadcast_queue.empty()):
            time.sleep(5)
        print ("request a new experiment")
        message = {
            'type': 'start_new_experiment'
        }
        global Shards_
        if len(Shards_) == 1:
            self.closed=True
            self.server_socket.close()
        else:
            self.finish_register =False
            self.node_addresses = []
            self.restart_program()
            ttk=self.shard_leaders
            self.shard_leaders=[]
            for address in ttk:
                self.send_message(address, message)
    def send_ack_message(self, address,log_message):
        message = {
            'type': 'ack',
            'log': log_message
        }
        self.send_message(address, message)
    def restart_program(self):
        global Shards_
        global f_
        if len(Shards_) == 1:
            return 
        else:
            f_=f_[1:]
            Shards_=Shards_[1:]
            self.num_shards = Shards_[0]
            self.shards = defaultdict(list)
            self.repeat_times = 0
            self.finished_times = []
            self.finished_ = defaultdict(set)
            self.messages_to_send=defaultdict(list)
            self.f = f_[0]
            self.BB = BB_[0]
@app.route('/logs', methods=['GET'])
def get_logs():
    return jsonify(logs)
@app.route('/logs_simplified', methods=['GET'])
def get_logs_simplified():
    return jsonify(logs_simplified)
@app.route('/exit___', methods=['GET'])
def exit__():
    self.server_socket.close()
    os.kill(os.getpid(), signal.SIGTERM)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the coordinator node.")
    parser.add_argument('--host', type=str, required=True, help="Coordinator host address")
    parser.add_argument('--port', type=int, required=True, help="Coordinator port number")
    parser.add_argument('--total_nodes', type=int, required=True, help="Total number of nodes")
    parser.add_argument('--num_shards', type=str, required=True, help="Number of shards")
    parser.add_argument('--f', type=str, required=True, help="Fraction of adversary")
    parser.add_argument('--bb', type=str, required=True, help="If using Byzantine Reliable broadcast")
    parser.add_argument('--repeat_times', type=int, required=True, help="repeat_times")
    parser.add_argument('--web_port',type=int,required=True, help="web port")
    args = parser.parse_args()
    address = (args.host, args.port)
    total_nodes = args.total_nodes
    num_shards = eval(args.num_shards)
    BB = eval(args.bb)
    f =  eval(args.f)
    try:
        global global_coordinator
        global_coordinator = Coordinator(address, total_nodes, f[0], BB[0], num_shards[0], args.repeat_times)
        Shards_ = num_shards
        f_ = f
        BB_= BB
        app.run(host='0.0.0.0', port=args.web_port)
    except Exception as e:
        print (f"Error!!! {e}")