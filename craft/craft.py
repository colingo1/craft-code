"""
cRaft Code

Code for running on agent in cRaft consensus.
Uses GRPC for communication between agents.

Authors: Colin Goldberg, Timothy Castiglia
"""
import sys
import os
import grpc
import threading
import multiprocessing
from concurrent import futures
import math
import time
import random
import socket
import pickle

class Message():
    def __init__(self, func, obj):
        self.func = func
        self.obj = obj

class LogEntry():
    def __init__(self, term, data, appendedBy, proposer):
        self.term = term;
        self.data = data;
        self.appendedBy = appendedBy
        self.proposer = proposer;

class Entry():
    def __init__(self, entry, index):
        self.entry = entry
        self.index = index

class Proposal():
    def __init__(self, entry, index, commitIndex, proposer):
        self.entry = entry;
        self.index = index
        self.commitIndex = commitIndex
        self.proposer = proposer;

class Entries():
    def __init__(self, term, leaderId, prevLogIndex,
            prevLogTerm, entries, leaderCommit):
        self.term = term;
        self.leaderId = leaderId;
        self.prevLogIndex = prevLogIndex;
        self.prevLogTerm = prevLogTerm;
        self.entries = entries;
        self.leaderCommit = leaderCommit;

class Ack():
    def __init__(self, term, success, server, level):
        self.term = term;
        self.success = success;
        self.server = server
        self.level = level

class VoteRequest():
    def __init__(self, term, candidateId, lastLogIndex, lastLogTerm):
        self.term = term;
        self.candidateId = candidateId;
        self.lastLogIndex = lastLogIndex;
        self.lastLogTerm = lastLogTerm;

DEBUG = True
# Stable storage of all servers as defined in the cRaft paper.
currentTerm = 0;
log = [[LogEntry(data = "NULL", term = 0, appendedBy = True, proposer="")],
        [LogEntry(data = "NULL", term = 0, appendedBy = True, proposer="")]]
votedFor = "";

# Volatile state of all servers
commitIndex = [0,0];
lastApplied = [0,0];
leaderId = [None,None];
lastGlobalIndex = 0
proposal_count = 0

# Volatile state of leaders
nextIndex = [{},{}]
matchIndex = [{},{}]
fastMatchIndex = [{},{}]

current_state = ["follower","follower"]

# Read from cluster.txt file for members of cluster
instance_file = open("cluster.txt", 'r')
members = [[],[]]
lines = instance_file.readlines()
for line in lines:
    members[0].append((line[0:-1],8100)) 
instance_file.close()

# Read from instances.txt file for members
instance_file = open("global.txt", 'r')
lines = instance_file.readlines()
for line in lines:
    members[1].append((line[0:-1],8100)) 
instance_file.close()

# Possible Entries structure
# List of lists, length of log
# Inner lists length of number of members
possibleEntries = [[[None]*len(members[0])], [[None]*len(members[1])]]

# Read in own host name
my_port = 8100 
host_file = open("host_name.txt", 'r')
this_id = (host_file.readlines()[0][0:-1],8100)
host_file.close()

# Create socket for listening and sending
sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP	
sock.bind(this_id)

os.system("touch /home/ubuntu/"+this_id[0]+".txt")

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

def ack_append(success, server, level=0):
    global sock
    new_message = Message("ACK_append", Ack(term = currentTerm, 
                    success = success, server = this_id, level = level))
    message_string = pickle.dumps(new_message)
    sock.sendto(message_string, server)

def ack(success, server, level=0):
    global sock
    new_message = Message("ACK", Ack(term = currentTerm, 
                    success = success, server = this_id, level = level))
    message_string = pickle.dumps(new_message)
    sock.sendto(message_string, server)

def term_equal(log_index, term, level=0):
    if len(log[level])-1 < log_index:
        return False
    return log[log_index].term == term

def insert_log(entry, index, appendedBy, level):
    global log
    while len(log[level]) <= index:
        log[level].append(None)
    entry.appendedBy = appendedBy
    log[level][index] = entry

def propose(entry, index, server, level=0): 
    global sock
    global commitIndex, this_id
    if server is None:
        return
    new_message = None
    if level == 0:
        new_message = Message("ReceivePropose", Proposal(entry = entry, 
                                           index = index,
                                           commitIndex = commitIndex[level],
                                           proposer = this_id))
    else:
        new_message = Message("GlobalReceivePropose", Proposal(entry = entry, 
                                           index = index,
                                           commitIndex = commitIndex[level],
                                           proposer = this_id))
    message_string = pickle.dumps(new_message)
    sock.sendto(message_string, server)

def GlobalReceivePropose(request):
    ReceivePropose(request,1)

def ReceivePropose(request,level=0):
    global log, possibleEntries, members, leaderId, current_state

    debug_print("Received Proposal from {} for index {}".format(request.proposer,request.index))
    if request.index >= len(log[level]) or log[level][request.index] == None:
        insert_log(request.entry, request.index, False, level)
        if level == 1:
            global_update_everyone(request.entry, request.index)
        if current_state[level] == "leader":
            # Add empty entries to log and possibleEntries
            while request.index >= len(possibleEntries[level]):
                possibleEntries[level].append([None]*len(members[level]))
            selfIndex = members[level].index(this_id)
            possibleEntries[level][request.index][selfIndex] = request.entry

    if current_state[level] == "leader":
        # Add empty entries to log and possibleEntries
        while request.index >= len(possibleEntries[level]):
            possibleEntries[level].append([None]*len(members[level]))

        # Add proposer's vote to possibleEntries
        proposerIndex = members[level].index(request.proposer)
        possibleEntries[level][request.index][proposerIndex] = request.entry
        nextIndex[level][request.proposer] = request.commitIndex+1
    else:
        propose(log[level][request.index], request.index, leaderId[level], level)

def GlobalAppendEntries(request):
    AppendEntries(request,1)

def AppendEntries(request,level=0):
    global log, commitIndex, currentTerm
    global timer, first, run, propose_time

    if first:
        first = False
        propose_time = True
        run = threading.Timer(60*3, stop_running)
        run.start()

    debug_print("Received AppendEntries from {}".format(request.leaderId))
    if request.term < currentTerm:
        ack(False, request.leaderId, level)
    #if not term_equal(request.prevLogIndex, request.prevLogTerm):
    #    return ack(False)
    leaderId[level] = request.leaderId

    if request.term > currentTerm:
        global current_state
        current_state[level] = "follower"
        currentTerm = request.term
        debug_print("Sending uncommitted entries to {}".format(request.leaderId))
        # This is a new leader, need to send uncommitted entries
        for i in range(commitIndex[level]+1, len(log[level])):
            propose(log[level][i], i, request.leaderId, level)
    
    # Overwrite existing entries
    i = 1
    for entry in request.entries:
        index = request.prevLogIndex+i
        insert_log(entry, index, True, level)
        if level == 1:
            global_update_everyone(entry, index)
        print("appended entry: {} to log in index {}".format(entry.data, index))
        i += 1

    oldCommitIndex = commitIndex[level]
    commitIndex[level] = min(request.leaderCommit, len(log[level]) -1)
    if commitIndex[level] > oldCommitIndex:
        debug_print("committing to {} at level {}".format(commitIndex[level], level))

    ack(True, request.leaderId, level)

def AppendEntry(request):
    insert_log(request.entry, request.index, True, 1)
    ack_append(True, request.leaderId, 1)

#def RequestVote(self,request,context):
#    global currentTerm, commitIndex
#    if request.term < currentTerm:
#        return ack(False)
#    if request.term > currentTerm:
#        global current_state
#        current_state = "follower"
#        currentTerm = request.term
#
#    # If haven't voted yet, and at least as up-to-date as self, vote for
#    if((votedFor == "" or votedFor == request.candidateId) and 
#         (request.lastLogIndex >= commitIndex)):
#            debug_print("Voted for {}".format(request.candidateId))
#            return ack(True)
#
#    # Do not vote for
#    return ack(False)

def Notified(request,level=0):
    global start_times, propose_time
    t = start_times[level][request.entry.data]
    elapsed_time = time.time() - t
    propose_time = True
    del repropose_log[level][request.entry.data]


def send_append_entries(server,level=0):
    global nextIndex, matchIndex, commitIndex, currentTerm
    global sock
    prev_index = nextIndex[level][server]-1
    prev_term = 0 
    if len(log[level]) > prev_index and prev_index >= 0:
        prev_term = log[level][prev_index].term 
    entries = log[level][prev_index+1:]
    debug_print("Sending AppendEntries to {} with prev_index {}".format(server,prev_index))
    new_message = None
    if level == 0:
        new_message = Message("AppendEntries",Entries(term = currentTerm, leaderId = this_id, prevLogIndex = prev_index, prevLogTerm = prev_term, entries=entries,leaderCommit = commitIndex[level]))
    else:
        new_message = Message("GlobalAppendEntries",Entries(term = currentTerm, leaderId = this_id, prevLogIndex = prev_index, prevLogTerm = prev_term, entries=entries,leaderCommit = commitIndex[level]))
    message_string = pickle.dumps(new_message)
    sock.sendto(message_string, server)

def AppendEntriesResp(response):
    global nextIndex, matchIndex, commitIndex, currentTerm, proposal_count
    level = response.level
    server = response.server
    if response.term > currentTerm:
        global current_state
        currentTerm = response.term
        current_state[level] = "follower"
        return False
    if not response.success:
        nextIndex[level][server] -=1
        send_append_entries(server,level)
    if response.success:
        nextIndex[level][server] = len(log[level])
        matchIndex[level][server] = len(log[level])-1

    new_commit_index = commitIndex[level]
    for i in range(commitIndex[level]+1,len(log[level])):
        greater_index = [index for index in matchIndex[level].values() if index >= i]
        if len(greater_index) > len(members[level])/2:
            debug_print("committing to {}".format(i))
            new_commit_index = i
            # Notify proposer
            notify(log[level][i].proposer, log[level][i])
            if level == 0:
                proposal_count += 1
    commitIndex[level] = new_commit_index

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
    global sock
    if server is None:
        return
    debug_print("Notifying {}".format(server))
    new_message = Message("Notified", Entry(entry = entry, index = -1))
    message_string = pickle.dumps(new_message)
    sock.sendto(message_string, server)

def update_entries(level=0):
    global commitIndex, possibleEntries, proposal_count

    # Fast-track commit check
    k = commitIndex[level]+1
    while(len(possibleEntries[level]) > k and
          sum(x is not None for x in possibleEntries[level][k]) > len(members[level])/2):
        # Count votes for each entry
        # Insert entry e with most votes
        e,count = most_frequent(possibleEntries[level][k])
        insert_log(e,k,True,level)
        if level == 1:
            global_update_everyone(e, k)

        # Update fastMatchIndex for agents
        for i in range(0,len(members[level])):
            if possibleEntries[level][k][i] == e:
                fastMatchIndex[level][members[level][i]] = k

        # If Fast-track succeeded
        if count >= math.ceil(3.0*len(members[level])/4.0):
             
            # Remove e from possibleEntries
            for j in range(k+1, len(possibleEntries[level])):
                for i in range(0,len(members[level])):
                    if possibleEntries[level][j][i] == e:
                        possibleEntries[level][j][i] = None

            # Update commitIndex and notify client
            commitIndex[level] = k
            if level == 0:
                proposal_count += 1
            notify(log[level][k].proposer, log[level][k])
            k += 1
        else: # Wait for this entry to be committed 
            break

    global poss_timer
    poss_timer = threading.Timer(75/1000.0, poss_timeout) 
    poss_timer.start()

def update_everyone(level=0):
    global commitIndex, possibleEntries, proposal_count

    update_entries()

    # Update followers 
    for server in members[level]:
        send_append_entries(server,level)

    global heartbeat_timer
    heartbeat_timer = threading.Timer(100/1000.0, heartbeat_timeout) 
    heartbeat_timer.start()


def become_leader(level=0):
    global nextIndex, matchIndex, election_timer
    #election_timer.cancel()
    nextIndex[level] = {member:len(log[level]) for member in members[level]}
    matchIndex[level] = {member:0 for member in members[level]}

    update_everyone(level)

appended_members = {}
appending = False

def global_update_everyone(entry, index):
    global members, appended_members, appending

    appending = True
    while len(appended_members) <= len(members[0])/2:
        for server in members[0]:
            new_message = Message("AppendEntry", Entry(entry = entry, index = index))
            message_string = pickle.dumps(new_message)
            sock.sendto(message_string, server)
        time.sleep(50/1000)

    appending = False
    appended_members = {}

def AppendEntryResp(request):
    global appending, appended_members
    if appending:
        appended_members[request.server] = True

#def hold_election():
#    global currentTerm,matchIndex,current_state,commitIndex
#    currentTerm += 1
#    votedFor = this_id
#    vote_count = 1
#    for server in members:
#        if server != this_id:
#            with grpc.insecure_channel(server) as channel:
#                stub = craft_pb2_grpc.cRaftStub(channel)
#                try:
#                    response = stub.RequestVote(craft_pb2.VoteRequest(term = currentTerm, candidateId = this_id, lastLogIndex = commitIndex, lastLogTerm = log[commitIndex].term))
#                    if response.success:
#                        vote_count +=1
#                        debug_print("received vote from {}".format(server))
#                    if response.term > currentTerm:
#                        current_state = "follower"
#                        currentTerm = response.term
#                except grpc.RpcError as e:
#                    debug_print(e)
#                    debug_print("couldn't connect to {}".format(server))
#    if vote_count >= len(members)/2:
#        current_state = "leader"
#        become_leader()
#    else:
#        debug_print("lost election")
#        current_state = "follower"
#        global election_timer
#        randTime = random.randint(250,500)
#        election_timer = threading.Timer(randTime/100.0, election_timeout) 
#        election_timer.start()

def propose_all(entry, index, level=0):
    global members, log, commitIndex, this_id, global_members
    for server in members[level]:
        propose(entry,index,server,level)

def receive_message(data):
    message = pickle.loads(data)
    if message.func == "ReceivePropose":
        ReceivePropose(message.obj)
    elif message.func == "GlobalReceivePropose":
        GlobalReceivePropose(message.obj)
    elif message.func == "AppendEntries":
        AppendEntries(message.obj)
    elif message.func == "GlobalAppendEntries":
        GlobalAppendEntries(message.obj)
    elif message.func == "AppendEntry":
        AppendEntry(message.obj)
    elif message.func == "ACK":
        AppendEntriesResp(message.obj)
    elif message.func == "ACK_append":
        AppendEntryResp(message.obj)
    #elif message.func == "RequestVote":
    #    RequestVote(message.obj)
    elif message.func == "Notified":
        Notified(message.obj)

def start_grpc_server():
    global sock
    while running:
            data, addr = sock.recvfrom(1048576) # buffer size is 2^20 bytes
            # To have a way to safely quit, quit message breaks loop
            try:
                if data.decode('UTF-8') == "quit":
                    break
            except UnicodeDecodeError:
                pass

            # Make thread for receive handling
            #receive_thread = multiprocessing.Process(
            receive_thread = threading.Thread(
                    target=receive_message,
                    args=(data,))
            receive_thread.start()


"""
Timer stop functions
"""

# Used in followers to decide if leader has failed 
#def election_timeout():
#    global current_state
#    debug_print("Election timeout")
#    current_state = "candidate"
#election_timer = threading.Timer(500/100.0, election_timeout) 
#election_timer.start()

# For reproposing entries
repropose_time = [True, True]
def repropose_timeout():
    global repropose_time
    for level in range(0,1):
        repropose_time[level] = True
repropose_timer = None 

# Used by leader to determine if it is time to send out heartbeat
update_poss = [False, False]
def poss_timeout():
    global update_poss
    for level in range(0,1):
        update_poss[level] = True
poss_timer = None 

# Used by leader to determine if it is time to send out heartbeat
update = [False, False]
def heartbeat_timeout():
    global update
    debug_print("Heartbeat timeout")
    for level in range(0,1):
        update[level] = True
heartbeat_timer = None 

# Used by all to determine if we want to propose a new entry 
propose_time = False
def propose_timeout():
    global propose_time
    debug_print("Proposal timeout")
    propose_time = True
repropose_log = [{}, {}]

# Run experiment for set amount of time
running = True
def stop_running():
    global running
    debug_print("Running timeout")
    running = False

# To time proposal turnaround time
start_times = [{},{}]

"""
Main loop
"""

def main(args):
    global update, propose_time, heartbeat_timer, proposal_timer
    global running, start_times, poss_timer, update_poss
    global current_state, log, lastGlobalIndex, proposal_count

    server_thread = threading.Thread(target=start_grpc_server,daemon=True)
    server_thread.start()
    counter = 0
    current_state[1] = args[2]
    current_state[0] = args[3]
    if current_state[0] == "leader":
        become_leader(0)
    if current_state[1] == "leader":
        become_leader(1)
    
    while running:
        for level in range(0,1):
            if repropose_time[level] and args[1] == "propose":
                try:
                    repropose_time[level] = False
                    for entry,index in repropose_log[level].values():
                        propose_all(entry, index, level)
                except: # if dictionary changes sizes in middle of run, don't panic
                    pass
                repropose_timer = threading.Timer(1000/1000.0, repropose_timeout) 
                repropose_timer.start()
            if current_state[level] == "leader" and update_poss[level]:
                update_poss[level] = False
                update_entries(level)
            if current_state[level] == "leader" and update[level]:
                update[level] = False
                update_everyone(level)
        if propose_time and args[1] == "propose":
            counter += 1
            propose_time = False
            entry = LogEntry(data = str(counter), 
                                          term = currentTerm,
                                          appendedBy = False,
                                          proposer = this_id)
            start_times[0][str(counter)] = time.time()
            index = len(log[0])
            propose_all(entry,index)
            repropose_log[0][entry.data] = (entry, index)
        if current_state[0] == "leader" and proposal_count >= 10:
            proposal_count = 0
            global_entries = log[0][lastGlobalIndex:]
            data = ','.join([x.data for x in global_entries])
            lastGlobalIndex = len(log[0])

            entry = LogEntry(data = data, 
                                  term = currentTerm,
                                  appendedBy = False,
                                  proposer = this_id)
            start_times[1][data] = time.time()
            index = len(log[1])
            propose_all(entry, index, 1)
            repropose_log[1][entry.data] = (entry, index)
        if current_state == "candidate":
            hold_election()

    # Count number of log entries that got into global log
    count = 0
    for e in log[1]:
        if e is None:
            continue
        count += len(e.data.split(','))

    # Save results
    f=open("/home/ubuntu/"+this_id[0]+".ind", "w")
    f.write(str(count-1))
    f.close()

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
#            entry = craft_pb2.LogEntry(data = command[8:], 
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
