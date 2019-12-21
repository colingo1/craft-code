"""
Raft Code

Code for running on agent in Raft consensus.
Uses GRPC for communication between agents.

Authors: Colin Goldberg, Timothy Castiglia
"""
import sys
import os
import grpc
import threading
from concurrent import futures

import raft_pb2
import raft_pb2_grpc

DEBUG = True
# Stable storage of all servers as defined in the Raft paper.
currentTerm = 0;
log = [raft_pb2.LogEntry(data = "NULL", term = 0)];
votedFor = "";

# Volatile state of all servers
commitIndex = 0;
lastApplied = 0;

# Volatile state of leaders
nextIndex = {}
matchIndex = {}

current_state = "follower"

# Read from instances.txt file for members
instance_file = open("instances.txt", 'r')
members = []
lines = instance_file.readlines()
for line in lines:
    members.append(line[0:-1]) 
instance_file.close()

# Read in own host name
my_port = 8100 
host_file = open("host_name.txt", 'r')
this_id = host_file.readlines()[0]
host_file.close()

cred_file = os.popen("ls /home/ubuntu").read()[0:-1]
with open("/home/ubuntu/"+cred_file, 'rb') as f:
    creds = grpc.ssl_channel_credentials(f.read())

def debug_print(m):
    if DEBUG:
        print(m)

debug_print(members)
debug_print(this_id)

def print_log():
    print("Log:")
    print("index\tterm\tdata")
    for i in range(1,len(log)):
        
        print("{}\t{}\t{}".format(i,log[i].term,log[i].data))

def ack(success):
    return raft_pb2.Ack(term = currentTerm, success = success)

def term_equal(log_index, term):
    if len(log)-1 < log_index:
        return False
    return log[log_index].term == term

def propose(entry): 
    global commitIndex
    with grpc.secure_channel(entry.leaderId, creds) as channel:
        stub = fraft_pb2_grpc.fRaftStub(channel)
        debug_print("Sending Proposal to {}".format(entry.leaderId))
        stub.ReceivePropose(fraft_pb2.Proposal(entry = entry, 
                                               proposer = this_id))

class Raft(raft_pb2_grpc.RaftServicer):

    def ReceivePropose(self,request,context):
        global log
        log.append(request.entry)

    def AppendEntries(self,request,context):
        global log, commitIndex, currentTerm

        debug_print("Received AppendEntries from {}".format(request.leaderId))
        if request.term < currentTerm:
            return ack(False)
        if not term_equal(request.prevLogIndex, request.prevLogTerm):
            return ack(False)
        if request.term > currentTerm:
            currentTerm = request.term
        
        log = log[:request.prevLogIndex+1]

        for entry in request.entries:
            log.append(entry)
            print("appended entry: {} to log in index {}".format(entry.data, len(log)-1))

        oldCommitIndex = commitIndex
        commitIndex = min(request.leaderCommit, len(log) -1)
        if commitIndex > oldCommitIndex:
            debug_print("committing to {}".format(commitIndex))

        return ack(True)


    def RequestVote(self,request,context):
        global currentTerm
        if request.term < currentTerm:
            return ack(False)
        if request.term > currentTerm:
            currentTerm = request.term

        #Haven't yet committed vote
        if (votedFor == "" or votedFor == request.candidateId):
            if (request.lastLogIndex >= len(log)-1 and request.lastLogTerm >= log[-1].term) or request.lastLogTerm > log[-1].term:
                debug_print("Voted for {}".format(request.candidateId))
                return ack(True)
        return ack(False)


def send_append_entries(server,heartbeat):
    global nextIndex, matchIndex, commitIndex, currentTerm
    with grpc.secure_channel(server, creds) as channel:
        try:
            stub = raft_pb2_grpc.RaftStub(channel)
            prev_index = nextIndex[server]-1
            prev_term = log[prev_index].term
            if heartbeat:
                entries = []
            else:
                entries = log[prev_index+1:]
            debug_print("Sending AppendEntries to {} with prev_index {}".format(server,prev_index))
            response = stub.AppendEntries(raft_pb2.Entries(term = currentTerm, leaderId = this_id, prevLogIndex = prev_index, prevLogTerm = prev_term, entries=entries,leaderCommit = commitIndex))
            if response.term > currentTerm:
                global current_state
                currentTerm = response.Term
                current_state = "follower"
                return False
            if not response.success:
                nextIndex[server] -=1
                send_append_entries(server,heartbeat)
            if response.success and not heartbeat:
                nextIndex[server] = len(log)
                matchIndex[server] = len(log)-1
        except grpc.RpcError as e:
            debug_print("couldn't connect to {}".format(server))
    return matchIndex[server]


def update_everyone(heartbeat):
    global commitIndex
    for server in members:
        send_append_entries(server,heartbeat)
    new_commit_index = commitIndex
    for i in range(commitIndex+1,len(log)):
        greater_index = [index for index in matchIndex.values() if index >= i]
        if len(greater_index) > len(members)/2:
            debug_print("committing to {}".format(i))
            new_commit_index = i
    commitIndex = new_commit_index


def become_leader():
    global nextIndex, matchIndex
    nextIndex = {member:len(log) for member in members}
    matchIndex = {member:0 for member in members}

    update_everyone(True)

def hold_election():
    global currentTerm,matchIndex,current_state
    currentTerm += 1
    votedFor = this_id
    vote_count = 1
    for server in members:
        if server != this_id:
            with grpc.secure_channel(server, creds) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                try:
                    response = stub.RequestVote(raft_pb2.VoteRequest(term = currentTerm, candidateId = this_id, lastLogIndex = len(log)-1, lastLogTerm = log[-1].term))
                    if response.success:
                        vote_count +=1
                        debug_print("received vote from {}".format(server))
                    if response.term > currentTerm:
                        currentTerm = response.term
                except grpc.RpcError as e:
                    debug_print("couldn't connect to {}".format(server))
    if vote_count >= len(members)/2:
        current_state = "leader"
        become_leader()
    else:
        debug_print("lost election")
        current_state = "follower"




def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(Raft(), server)
    server.add_secure_port('[::]:{}'.format(my_port), creds)
    server.start()
    server.wait_for_termination()


#an original plan for an automatic loop
# while(True):
#     if(current_state == "leader"):
#         global nextIndex, matchIndex
#         nextIndex = {member:len(log) for member in members}
#         matchIndex = {member:0 for member in members}

#         for server in members:
#             send_append_entries(server,True)
            

#     if(current_state == "candidate"):
#         global currentTerm,votedFor
#         currentTerm += 1
#         for server in 


#     if(current_state == "follower"):



def main():
    server_thread = threading.Thread(target=start_grpc_server,daemon=True)
    server_thread.start()

    while True:
        global current_state
        # TODO make automatic 
        command = input()
        if command == "status":
            print("current state of {}: {}".format(this_id,current_state))
            print_log()
            print("Current commitIndex: {}".format(commitIndex))
        # TODO measure turnaround time on propose
        # TODO save results in /home/ubuntu/{host_name}.txt
        if command[:7] == "propose":
            if current_state == "leader":
                log.append(raft_pb2.LogEntry(data = command[8:], term = currentTerm))
            else:
                print("Not leader, can't propose")
        if command == "update":
            if current_state == "leader":
                update_everyone(False)
        # TODO randomized timeout for election if no heartbeat
        if command == "elect":
            if current_state == "follower":
                current_state = "candidate"
                hold_election()


if __name__ == '__main__':
    main()
