"""
fRaft Code

Code for running on agent in fRaft consensus.
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

import fraft_pb2
import fraft_pb2_grpc

DEBUG = True
# Stable storage of all servers as defined in the fRaft paper.
currentTerm = 0;
log = [fraft_pb2.LogEntry(data = "NULL", term = 0, appendedBy = True)];
votedFor = "";

# Volatile state of all servers
commitIndex = 0;
lastApplied = 0;
leaderId = "";

# Volatile state of leaders
nextIndex = {}
matchIndex = {}
fastMatchIndex = {}

current_state = "follower"

# Read from instances.txt file for members
instance_file = open("instances.txt", 'r')
members = []
lines = instance_file.readlines()
for line in lines:
    members.append(line[0:-1]+":8100") 
instance_file.close()

# Possible Entries structure
# List of lists, length of log
# Inner lists length of number of members
possibleEntries = [[None]*len(members)]

# Read in own host name
my_port = 8100 
host_file = open("host_name.txt", 'r')
this_id = host_file.readlines()[0][0:-1]+":8100"
host_file.close()

os.system("touch /home/ubuntu/"+this_id+".txt")

first = True

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
    return fraft_pb2.Ack(term = currentTerm, success = success)

def term_equal(log_index, term):
    if len(log)-1 < log_index:
        return False
    return log[log_index].term == term

def insert_log(entry, index, appendedBy):
    global log
    while len(log) <= index:
        log.append(None)
    entry.appendedBy = appendedBy
    log[index] = entry

def propose(entry, index, p_server): 
    global commitIndex, this_id
    if p_server == "":
        return
    with grpc.insecure_channel(p_server) as p_channel:
        try:
            p_stub = fraft_pb2_grpc.fRaftStub(p_channel)
            debug_print("Sending Proposal to {} with index {}".format(p_server,index))
            p_stub.ReceivePropose(fraft_pb2.Proposal(entry = entry, 
                                                   index = index,
                                                   commitIndex = commitIndex,
                                                   proposer = this_id))
        except grpc.RpcError as e:
            debug_print(e)
            debug_print("couldn't connect to {}".format(p_server))

class fRaft(fraft_pb2_grpc.fRaftServicer):

    def ReceivePropose(self,request,context):
        global log, possibleEntries, members, leaderId, current_state

        debug_print("Received Proposal from {} for index {}".format(request.proposer,request.index))
        if request.index >= len(log) or log[request.index] == None:
            insert_log(request.entry, request.index, False)

        if current_state == "leader":
            # Add empty entries to log and possibleEntries
            while request.index >= len(possibleEntries):
                possibleEntries.append([None]*len(members))

            # Add proposer's vote to possibleEntries
            proposerIndex = members.index(request.proposer)
            possibleEntries[request.index][proposerIndex] = request.entry
            nextIndex[request.proposer] = request.commitIndex+1
        else:
            propose(log[request.index], request.index, leaderId)
        return ack(True)

    def AppendEntries(self,request,context):
        global log, commitIndex, currentTerm, leaderId
        global election_timer, first, run, propose_time

        if first:
            first = False
            propose_time = True
            run = threading.Timer(60*3, stop_running)
            run.start()


        debug_print("Received AppendEntries from {}".format(request.leaderId))
        if request.term < currentTerm:
            return ack(False)
        #if not term_equal(request.prevLogIndex, request.prevLogTerm):
        #    return ack(False)
        leaderId = request.leaderId
        election_timer.cancel()
        randTime = random.randint(150,300)
        election_timer = threading.Timer(randTime/100.0, election_timeout) 
        election_timer.start()
        if request.term > currentTerm:
            currentTerm = request.term
            debug_print("Sending uncommitted entries to {}".format(request.leaderId))
            # This is a new leader, need to send uncommitted entries
            for i in range(commitIndex+1, len(log)):
                propose(log[i], i, request.leaderId)
        
        # Overwrite existing entries
        i = 1
        for entry in request.entries:
            index = request.prevLogIndex+i
            insert_log(entry, index, True)
            print("appended entry: {} to log in index {}".format(entry.data, index))
            i += 1

        oldCommitIndex = commitIndex
        commitIndex = min(request.leaderCommit, len(log) -1)
        if commitIndex > oldCommitIndex:
            debug_print("committing to {}".format(commitIndex))

        return ack(True)


    def RequestVote(self,request,context):
        global currentTerm, commitIndex
        if request.term < currentTerm:
            return ack(False)
        if request.term > currentTerm:
            currentTerm = request.term

        # If haven't voted yet, and at least as up-to-date as self, vote for
        if((votedFor == "" or votedFor == request.candidateId) and 
             (request.lastLogIndex >= commitIndex)):
                debug_print("Voted for {}".format(request.candidateId))
                return ack(True)

        # Do not vote for
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
    global nextIndex, matchIndex, commitIndex, currentTerm
    with grpc.insecure_channel(server) as channel:
        try:
            stub = fraft_pb2_grpc.fRaftStub(channel)
            prev_index = nextIndex[server]-1
            prev_term = 0 
            if heartbeat:
                entries = []
            else:
                entries = log[prev_index+1:]
            debug_print("Sending AppendEntries to {} with prev_index {}".format(server,prev_index))
            response = stub.AppendEntries(fraft_pb2.Entries(term = currentTerm, leaderId = this_id, prevLogIndex = prev_index, prevLogTerm = prev_term, entries=entries,leaderCommit = commitIndex))
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

def most_frequent(List): 
    List = [x for x in List if x is not None]
    counter = 0
    num = List[0] 
    for i in List: 
        curr_frequency = 0
        # Count number of votes for this entry
        for j in List:
            if i.data == j.data:
                curr_frequency += 1 

        # If there is a leader-approved entry, append that
        if i.appendedBy:
            return i,curr_frequency

        # Update max frequency
        if(curr_frequency > counter): 
            counter = curr_frequency 
            num = i 
    return(num, counter) 

def notify(server, entry):
    if server == "":
        return
    with grpc.insecure_channel(server) as channel:
        try:
            stub = fraft_pb2_grpc.fRaftStub(channel)
            debug_print("Notifying {}".format(server))
            stub.Notified(fraft_pb2.Entry(entry = entry))
        except grpc.RpcError as e:
            debug_print(e)
            debug_print("couldn't connect to {}".format(p_server))


def update_everyone(heartbeat):
    global commitIndex, possibleEntries

    # Fast-track commit check
    k = commitIndex+1
    while(len(possibleEntries) > k and
          sum(x is not None for x in possibleEntries[k]) > len(members)/2):
        # Count votes for each entry
        # Insert entry e with most votes
        e,count = most_frequent(possibleEntries[k])
        insert_log(e,k,True)

        # Update fastMatchIndex for agents
        for i in range(0,len(members)):
            if possibleEntries[k][i] == e:
                fastMatchIndex[members[i]] = k

        # If Fast-track succeeded
        if count >= math.ceil(3.0*len(members)/4.0):
             
            # Remove e from possibleEntries
            for j in range(k+1, len(possibleEntries)):
                for i in range(0,len(members)):
                    if possibleEntries[j][i] == e:
                        possibleEntries[j][i] = None

            # Update commitIndex and notify client
            commitIndex = k
            notify(log[k].proposer, log[k])
            k += 1
        else: # Wait for this entry to be committed 
            break

    # Update followers 
    for server in members:
        send_append_entries(server,heartbeat)
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
    election_timer.cancel()
    nextIndex = {member:len(log) for member in members}
    matchIndex = {member:0 for member in members}

    update_everyone(True)

def hold_election():
    global currentTerm,matchIndex,current_state,commitIndex
    currentTerm += 1
    votedFor = this_id
    vote_count = 1
    for server in members:
        if server != this_id:
            with grpc.insecure_channel(server) as channel:
                stub = fraft_pb2_grpc.fRaftStub(channel)
                try:
                    response = stub.RequestVote(fraft_pb2.VoteRequest(term = currentTerm, candidateId = this_id, lastLogIndex = commitIndex, lastLogTerm = log[commitIndex].term))
                    if response.success:
                        vote_count +=1
                        debug_print("received vote from {}".format(server))
                    if response.term > currentTerm:
                        currentTerm = response.term
                except grpc.RpcError as e:
                    debug_print(e)
                    debug_print("couldn't connect to {}".format(server))
    if vote_count >= len(members)/2:
        current_state = "leader"
        become_leader()
    else:
        debug_print("lost election")
        current_state = "follower"
        global election_timer
        randTime = random.randint(150,300)
        election_timer = threading.Timer(randTime/100.0, election_timeout) 
        election_timer.start()

def propose_all(entry):
    global members, log, commitIndex, this_id
    index = len(log)
    for server in members:
        with grpc.insecure_channel(server) as channel:
            stub = fraft_pb2_grpc.fRaftStub(channel)
            try:
                stub.ReceivePropose(fraft_pb2.Proposal(entry = entry, 
                                               index = index,
                                               commitIndex = commitIndex,
                                               proposer = this_id))
            except grpc.RpcError as e:
                debug_print(e)
                debug_print("couldn't connect to {}".format(server))

def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fraft_pb2_grpc.add_fRaftServicer_to_server(fRaft(), server)
    server.add_insecure_port('[::]:{}'.format(my_port))
    server.start()
    server.wait_for_termination()


"""
Timer stop functions
"""

# Used in followers to decide if leader has failed 
def election_timeout():
    global current_state
    debug_print("Election timeout")
    current_state = "candidate"
election_timer = threading.Timer(150/100.0, election_timeout) 
election_timer.start()

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
    global running, start_times

    server_thread = threading.Thread(target=start_grpc_server,daemon=True)
    server_thread.start()
    counter = 0
    while running:
        global current_state, log
        if current_state == "leader" and update:
            update = False
            update_everyone(False)
        if propose_time and len(args) == 2:
            propose_time = False
            entry = fraft_pb2.LogEntry(data = str(counter), 
                                          term = currentTerm,
                                          appendedBy = True,
                                          proposer = this_id)
            start_times[str(counter)] = time.time()
            propose_all(entry)
            if args[1] == "propose":
                randTime = random.randint(50,100)
                proposal_timer = threading.Timer(randTime/100.0, propose_timeout) 
                proposal_timer.start()
        if current_state == "candidate":
            hold_election()
        counter += 1
        time.sleep(5/1000.0)

#def main():
#    server_thread = threading.Thread(target=start_grpc_server,daemon=True)
#    server_thread.start()
#
#    while True:
#        global current_state, log
#        # TODO make automatic 
#        command = input()
#        if command == "status":
#            print("current state of {}: {}".format(this_id,current_state))
#            print_log()
#            print("Current commitIndex: {}".format(commitIndex))
#        # TODO measure turnaround time on propose
#        # TODO save results in /home/ubuntu/{host_name}.txt
#        if command[:7] == "propose":
#            entry = fraft_pb2.LogEntry(data = command[8:], 
#                                          term = currentTerm,
#                                          appendedBy = True)
#            propose_all(entry)
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
