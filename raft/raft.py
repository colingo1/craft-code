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
import math
import time
import random
import multiprocessing

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
leaderId = "";

# Volatile state of leaders
nextIndex = {}
matchIndex = {}

current_state = "follower"

# Read from instances.txt file for members
instance_file = open("instances.txt", 'r')
members = []
lines = instance_file.readlines()
for line in lines:
    members.append(line[0:-1]+":8100") 
instance_file.close()

# Read in own host name
my_port = 8100 
host_file = open("host_name.txt", 'r')
this_id = host_file.readlines()[0][0:-1]+":8100"
host_file.close()

os.system("touch /home/ubuntu/"+this_id+".txt")

first = True
TIMEOUT = 0.3

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
    if log_index < 0:
        return True
    if len(log)-1 < log_index:
        return False
    return log[log_index].term == term

def propose(entry, server): 
    global commitIndex
    if server == "":
        return
    with grpc.insecure_channel(server) as channel:
        try:
            stub = raft_pb2_grpc.RaftStub(channel)
            debug_print("Sending Proposal to {}".format(server))
            response = stub.ReceivePropose(raft_pb2.Proposal(entry = entry, 
                                                   proposer = this_id), timeout=TIMEOUT)
        except grpc.RpcError as e:
            debug_print(e)
            debug_print("couldn't connect to {}".format(server))

class Raft(raft_pb2_grpc.RaftServicer):

    def ReceivePropose(self,request,context):
        global log
        log.append(request.entry)
        return ack(True)

    def AppendEntries(self,request,context):
        global log, commitIndex, currentTerm, leaderId
        global election_timer, first, run, propose_time


        debug_print("Received AppendEntries from {}".format(request.leaderId))
        if request.term < currentTerm:
            return ack(False)
        if not term_equal(request.prevLogIndex, request.prevLogTerm):
            return ack(False)
        leaderId = request.leaderId

        if first:
            first = False
            propose_time = True
            run = threading.Timer(60*3, stop_running)
            run.start()

        if leaderId != this_id:
            pass
            #election_timer.cancel()
            #randTime = random.randint(250,500)
            #election_timer = threading.Timer(randTime/100.0, election_timeout) 
            #election_timer.start()
        if request.term > currentTerm:
            global current_state
            current_state = "follower"
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
            global current_state
            current_state = "follower"
            currentTerm = request.term

        #Haven't yet committed vote
        if (votedFor == "" or votedFor == request.candidateId):
            if (request.lastLogIndex >= len(log)-1 and request.lastLogTerm >= log[-1].term) or request.lastLogTerm > log[-1].term:
                debug_print("Voted for {}".format(request.candidateId))
                return ack(True)
        return ack(False)

    def Notified(self,request,context):
        global start_times, propose_time
        t = start_times[request.entry.data]
        elapsed_time = time.time() - t
        f=open("/home/ubuntu/"+this_id+".txt", "a+")
        f.write(str(elapsed_time)+"\n")
        f.close()
        propose_time = True
        return ack(True)

def send_append_entries(server,heartbeat):
    global nextIndex, matchIndex, commitIndex, currentTerm, log
    with grpc.insecure_channel(server) as channel:
        try:
            stub = raft_pb2_grpc.RaftStub(channel)
            prev_index = nextIndex[server]-1
            prev_term = 0
            if len(log) > prev_index and prev_index >= 0:
                prev_term = log[prev_index].term
            if heartbeat:
                entries = []
            else:
                entries = log[prev_index+1:]
            debug_print("Sending AppendEntries to {} with prev_index {}".format(server,prev_index))
            response = stub.AppendEntries(raft_pb2.Entries(term = currentTerm, leaderId = this_id, prevLogIndex = prev_index, prevLogTerm = prev_term, entries=entries,leaderCommit = commitIndex), timeout=TIMEOUT)
            if response.term > currentTerm:
                global current_state
                currentTerm = response.term
                current_state = "follower"
                return False
            if not response.success:
                nextIndex[server] -=1
                send_append_entries(server,heartbeat)
            if response.success and not heartbeat:
                nextIndex[server] = len(log)
                matchIndex[server] = len(log)-1
        except grpc.RpcError as e:
            debug_print(e)
            debug_print("couldn't connect to {}".format(server))
    return matchIndex[server]


def notify(server, entry):
    if server == "":
        return
    with grpc.insecure_channel(server) as channel:
        try:
            stub = raft_pb2_grpc.RaftStub(channel)
            debug_print("Notifying {}".format(server))
            response = stub.Notified(raft_pb2.Entry(entry = entry), timeout=TIMEOUT)
        except grpc.RpcError as e:
            debug_print(e)
            debug_print("couldn't connect to {}".format(server))


def update_everyone(heartbeat):
    global commitIndex

    # Send append entries in parallel
    processes = []
    for server in members:
        p = multiprocessing.Process(target=send_append_entries, 
                args=(server,heartbeat))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

    new_commit_index = commitIndex
    for i in range(commitIndex+1,len(log)):
        greater_index = [index for index in matchIndex.values() if index >= i]
        if len(greater_index) > len(members)/2:
            debug_print("committing to {}".format(i))
            new_commit_index = i
            # Notify proposer
            notify(log[i].proposer, log[i])
    commitIndex = new_commit_index

    global heartbeat_timer
    heartbeat_timer = threading.Timer(50/100.0, heartbeat_timeout) 
    heartbeat_timer.start()


def become_leader():
    global nextIndex, matchIndex, election_timer
    #election_timer.cancel()

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
            with grpc.insecure_channel(server) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                try:
                    response = stub.RequestVote(raft_pb2.VoteRequest(term = currentTerm, candidateId = this_id, lastLogIndex = len(log)-1, lastLogTerm = log[-1].term), timeout=TIMEOUT)
                    if response.success:
                        vote_count +=1
                        debug_print("received vote from {}".format(server))
                    if response.term > currentTerm:
                        current_state = "follower"
                        currentTerm = response.term
                except grpc.RpcError as e:
                    debug_print("couldn't connect to {}".format(server))
    if vote_count >= len(members)/2:
        current_state = "leader"
        become_leader()
    else:
        debug_print("lost election")
        current_state = "follower"
        global election_timer
        randTime = random.randint(250,500)
        election_timer = threading.Timer(randTime/100.0, election_timeout) 
        election_timer.start()

def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=200))
    raft_pb2_grpc.add_RaftServicer_to_server(Raft(), server)
    for i in range(0, len(members)+1):
        server.add_insecure_port('[::]:{}'.format(my_port+i))
    server.start()
    server.wait_for_termination()


"""
Timer stop functions
"""

# Used in followers to decide if leader has failed 
#def election_timeout():
#    global current_state
#    debug_print("Election timeout")
#    current_state = "candidate"
#election_timer = threading.Timer(1000/100.0, election_timeout) 
#election_timer.start()

# Used by leader to determine if it is time to send out heartbeat
update = False
def heartbeat_timeout():
    global update
    debug_print("Heartbeat timeout")
    update = True
heartbeat_timer = None 

# Used by all to determine if we want to propose a new entry 
propose_time = False
def propose_timeout():
    global propose_time
    debug_print("Proposal timeout")
    propose_time = True

# Run experiment for set amount of time
running = True
def stop_running():
    global running
    debug_print("Running timeout")
    running = False

# To time proposal turnaround time
start_times = {}

"""
Main loop
"""

def main(args):
    global update, propose_time, election_timer, heartbeat_timer, proposal_timer
    global running, start_times, leaderId
    global current_state, log

    server_thread = threading.Thread(target=start_grpc_server,daemon=True)
    server_thread.start()
    counter = 0
    current_state = args[2]
    if current_state == "leader":
        become_leader()
    
    while running:
        if current_state == "leader" and update:
            update = False
            update_everyone(False)
        if propose_time and args[1] == "propose":
            propose_time = False
            entry = raft_pb2.LogEntry(data = str(counter), 
                                          term = currentTerm,
                                          proposer = this_id)
            start_times[str(counter)] = time.time()
            debug_print("Proposing entry {} to {}".format(entry.data,leaderId))
            propose(entry, leaderId)
            #if args[1] == "propose":
            #    randTime = random.randint(200,500)
            #    proposal_timer = threading.Timer(randTime/100.0, propose_timeout) 
            #    proposal_timer.start()
        if current_state == "candidate":
            hold_election()
        counter += 1


    # Count number of log entries that got into global log
    # Save results
    f=open("/home/ubuntu/"+this_id+".ind", "w")
    f.write(str(len(log)-1))
    f.close()

#def main():
#    server_thread = threading.Thread(target=start_grpc_server,daemon=True)
#    server_thread.start()
#
#    while True:
#        global current_state
#        # TODO make automatic 
#        command = input()
#        if command == "status":
#            print("current state of {}: {}".format(this_id,current_state))
#            print_log()
#            print("Current commitIndex: {}".format(commitIndex))
#        # TODO measure turnaround time on propose
#        # TODO save results in /home/ubuntu/{host_name}.txt
#        if command[:7] == "propose":
#            if current_state == "leader":
#                log.append(raft_pb2.LogEntry(data = command[8:], term = currentTerm))
#            else:
#                print("Not leader, can't propose")
#        if command == "update":
#            if current_state == "leader":
#                update_everyone(False)
#        # TODO randomized timeout for election if no heartbeat
#        if command == "elect":
#            if current_state == "follower":
#                current_state = "candidate"
#                hold_election()


if __name__ == '__main__':
    main(sys.argv)
