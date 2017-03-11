import socket
import threading
import socketserver
import sys
import queue
import select
import time
import message
import config 
import random
import log
STATE_FOLLOWER = 0
STATE_CANDIDATE = 1
STATE_LEADER = 2

debug = True
run_server = True
#server priority queue
pq = queue.PriorityQueue()
pq_lock = threading.RLock()
lclock = 0
lclock_lock = threading.RLock()
ticket_lock = threading.RLock()
election_lock = threading.Lock()
append_lock = None
cfg = None
tickets = None
delay = None
currentTerm = 0
votedFor = None
myLog = None
currentLeader = None
myState = STATE_FOLLOWER
my_id = None
timeout_interval = None
follower_timer = None
server_addr = None

class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
	
	def handle(self):
		message_in = recieve_message(self.request)
		if message_in is None:
			return
		cur_thread = threading.current_thread()
		response_message = handle_message(message_in, self.request)
		send_message(self.request, response_message)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
	
	def __exit__(self):
		self.shutdown()

def recieve_message(a_socket):
	try:
		m_in = message.Message.deserialize(a_socket.recv(2048))
	except EOFError:
		if(debug):
			print("Line 53: EOFERROR")
		return None
	#time.sleep(delay)
	if type(m_in) is message.AppendEntriesResponse or (type(m_in) is message.AppendEntries and m_in.entries is None):
		return m_in
	else:
		print("Recieved message of type: %s from %s" % (str(type(m_in)), str(a_socket.getpeername())))
	return m_in

def send_message(a_socket, m_out = None):
	if m_out is not None:
		a_socket.send(m_out.serialize())
		if type(m_out) is message.AppendEntriesResponse or (type(m_out) is message.AppendEntries and m_out.entries is None):
			return
		else:
			print("Sent message of type: %s to %s" % (str(type(m_out)), str(a_socket.getpeername())))


def get_kiosk_number():
	if len(sys.argv) < 2:
		print("Not enough args.")
		exit()
	else:
		kiosk_num = int(sys.argv[1])
		return kiosk_num

def update_tickets(val):
	global tickets
	with ticket_lock:
		tickets = val
		print("Updated ticket pool: %d" % tickets)

def resetElectionTimeout():
	global follower_timer
	follower_timer.cancel()
	follower_timer = threading.Timer(1, setCandidate)
	follower_timer.start()
	return
	
def setCandidate():
	global myState
	global votedFor
	global currentTerm
	if myState == STATE_FOLLOWER:
		follower_timer.cancel()
	myState = STATE_CANDIDATE
	setTerm(currentTerm + 1)
	votedFor = my_id
	holdElection()
	return
def setLeader():
	global myState
	if myState == STATE_CANDIDATE:
		myState = STATE_LEADER
	else:
		print("reached leader state from unexpected state")
	for x in range(0, len(myLog.getConfig().kiosks)):
		if x is not my_id:
			t = threading.Thread(target=sendHeartbeat, args = (myLog.getConfig().kiosks[x],))
			t.start()
	return
	
def sendHeartbeat(kiosk):
	global myState
	global myLog
	global currentTerm
	while(myState == STATE_LEADER):
		#append_lock[kiosk].acquire()
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			done = False
			s.connect(kiosk)
			prevIndex = myLog.getIndex() - 1
			prevTerm = myLog.getTerm(prevIndex)
			#print("AppendEntries: term=" + str(currentTerm) + " my_id=" + str(my_id) + " prevIndex=" + str(prevIndex) + " prevTerm=" + str(prevTerm) + " commit=" + str(myLog.getCommit()))
			send_message(s, message.AppendEntries(currentTerm, my_id, prevIndex, prevTerm, None, myLog.getCommit()))
			response = recieve_message(s)
			assert response is None or type(response) is message.AppendEntriesResponse
			if response.term > currentTerm:
				currentTerm = response.term			
			if response.success:
				done = True
			index = myLog.getIndex() - 1
			while not done:
				prevIndex = index - 1
				prevTerm = myLog.getTerm(prevIndex)
				entry = myLog.getEntry(index)
				#print("AppendEntries: term=" + str(currentTerm) + " my_id=" + str(my_id) + " prevIndex=" + str(prevIndex) + " prevTerm=" + str(prevTerm) + " entry=" + str(entry.command.num_tickets) + " commit=" + str(myLog.getCommit()))				
				send_message(s, message.AppendEntries(currentTerm, my_id, prevIndex, prevTerm, entry, myLog.getCommit()))
				response = recieve_message(s)
				assert response is None or  type(response) is message.AppendEntriesResponse
				if response.term > currentTerm:
					currentTerm = response.term						
				if response is None:
					pass
				elif response.success:
					done = True
				else:
					index = index - 1
		except ConnectionError:
			pass
		#append_lock[kiosk].release()
		time.sleep(0.5)
	
def setFollower():
	global myState
	if myState == STATE_CANDIDATE:
		pass
	elif myState == STATE_LEADER:
		pass
	elif myState == STATE_FOLLOWER:
		pass
	myState = STATE_FOLLOWER
	resetElectionTimeout()
	if(debug):
		print("Entering follower mode")
	return
	
def setTerm(newTerm):
	global votedFor
	global currentTerm
	print("Incrementing term: " + str(currentTerm))
	currentTerm = newTerm
	votedFor = None
def holdElection():
	election_lock.acquire()
	global currentTerm
	# start election
	if(debug):
		print("Follower timeout exceeded, starting election")
	#setCandidate()
	global myState
	global votedFor
	while myState == STATE_CANDIDATE:
		setTerm(currentTerm + 1)
		votedFor = my_id
		readers, writers, errors = [],[],[]
		currentConfig = myLog.getConfig()
		our_sockets = [None]*len(currentConfig.kiosks)
		sock_map = {}
		for x in range(0, len(currentConfig.kiosks)):
			if x is not my_id:
				try:
					our_sockets[x] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					our_sockets[x].connect(currentConfig.kiosks[x])
					our_sockets[x].setblocking(0)
					sock_map[our_sockets[x]] = currentConfig.kiosks[x]
					writers.append(our_sockets[x])
					readers.append(our_sockets[x])
				except:
					pass
	#	while len(writers) != 0:
	#		_ , pwriters , _ = select.select(readers, writers, errors)
	#		for writer in pwriters:
	#			send_message(writer, message.RequestVote(my_id, currentTerm, myLog.getIndex(), myLog.getTerm()))
	#			writers.remove(writer)
		denied = False
		#numVotes = 1
		voters = [server_addr]
		start = time.time()
		mytimeout = random.uniform(3.0,5.0)
		while (time.time() - start) < mytimeout:
			try:
				preaders, pwriters , _ = select.select(readers, writers, errors)
			except OSError:
				preaders = []
				pwriters = []
			for writer in pwriters:
				send_message(writer, message.RequestVote(my_id, currentTerm, myLog.getIndex(), myLog.getTerm()))
				writers.remove(writer)
			for reader in preaders:
				message_in = recieve_message(reader)
				assert message_in is None or type(message_in) is message.RequestVoteResponse
				readers.remove(reader)
				if message_in is None:
					print("Unexpected: received none")
					continue
				if message_in.voteGranted == False:
					denied = True
					break
				elif message_in.voteGranted == True:
					#numVotes = numVotes + 1
					voters.append(sock_map[reader])
				if currentConfig.hasQuorum(voters):#numVotes > len(currentConfig.kiosks)/2:
					break
			if currentConfig.hasQuorum(voters):#numVotes >= len(currentConfig.kiosks)/2:
				if(debug):
					print("I was elected leader with " + str(len(cfg.kiosks)/2) + "votes")
				setLeader()
				for s in our_sockets:
					if s is not None:
						try:
							s.close()
						except:
							pass
				election_lock.release()
				return
	election_lock.release()
	
def leaderTimeout():
	return
	
def sync_lclock(clock_val = None):
	global lclock
	with lclock_lock:
		if clock_val is not None and clock_val >= lclock:
			lclock = clock_val + 1
		else:
			lclock = lclock + 1
		print("Updated lamport_clock, new value: %d" % lclock)

		
def broadcastAppend(event, myConfig):
	global currentTerm
	global myLog
	if myState != STATE_LEADER:
		#append_lock.release()
		return False
	newLogEntry = log.LogEntry(currentTerm, myLog.getIndex(), event)
	myLog.appendEntry(newLogEntry)
	numKiosks = len(myConfig.kiosks)
	our_sockets = [None]*numKiosks
	readers, writers, errors = [],[],[]
	sock_map = {}
	for x in range(0, numKiosks):
		if x is not my_id:
			try:
				our_sockets[x] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				our_sockets[x].connect(myConfig.kiosks[x])
				our_sockets[x].setblocking(0)
				sock_map[our_sockets[x]] = x
				writers.append(our_sockets[x])
				readers.append(our_sockets[x])
			except:
				pass
	entry_index = [newLogEntry.index]*len(our_sockets)
	voters = [server_addr]
	while len(writers) != 0 or len(readers) != 0:
		preaders , pwriters , _ = select.select(readers, writers, errors)
		for writer in pwriters:
			w_id = sock_map[writer]
			index = entry_index[w_id]
			prevIndex = index - 1
			prevTerm = myLog.getTerm(prevIndex)
			entry = myLog.getEntry(index)
			send_message(writer, message.AppendEntries(currentTerm, my_id, prevIndex, prevTerm, entry, myLog.getCommit()))
			#print("AppendEntries: term=" + str(currentTerm) + " my_id=" + str(my_id) + " prevIndex=" + str(prevIndex) + " prevTerm=" + str(prevTerm) + " entry=" + str(entry.command.num_tickets) + " commit=" + str(myLog.getCommit()))
			writers.remove(writer)

		for reader in preaders:
			message_in = recieve_message(reader)
			assert type(message_in) is message.AppendEntriesResponse
			if message_in.term > currentTerm:
				currentTerm = message_in.term
			if message_in.success:
				readers.remove(reader)
				voters.append(myConfig.kiosks[sock_map[reader]])
			else:
				entry_index[sock_map[reader]] = entry_index[sock_map[reader]] - 1
				writers.append(reader)
	if myConfig.hasQuorum(voters):
		success = myLog.setCommit(newLogEntry.index)
		return success
	else:
		return False
		
		
def handle_message(our_message, our_socket):
	global currentTerm
	global tickets
	global votedFor
	global myState
	# if client message
		# if buy request
		
	if type(our_message) is message.ClientBuyRequest:
		#append_lock.acquire()
		if myState != STATE_LEADER:
			#append_lock.release()
			return message.ClientBuyResponse(tickets, False, currentLeader)
			
		success = broadcastAppend(our_message, myLog.getConfig())
		return message.ClientBuyResponse(myLog.getTickets(), success)
			
		newLogEntry = log.LogEntry(currentTerm, myLog.getIndex(), our_message)
		myLog.appendEntry(newLogEntry)
		our_sockets = [None]*message.TOTAL_KIOSKS
		readers, writers, errors = [],[],[]
		sock_map = {}
		for x in range(0, message.TOTAL_KIOSKS):
			if x is not my_id:
				try:
					our_sockets[x] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					our_sockets[x].connect(cfg.kiosks[x])
					our_sockets[x].setblocking(0)
					sock_map[our_sockets[x]] = x
					writers.append(our_sockets[x])
					readers.append(our_sockets[x])
				except:
					pass
		entry_index = [newLogEntry.index]*len(our_sockets)
		numVotes = 1
		while len(writers) != 0 or len(readers) != 0:
			preaders , pwriters , _ = select.select(readers, writers, errors)
			for writer in pwriters:
				w_id = sock_map[writer]
				index = entry_index[w_id]
				prevIndex = index - 1
				prevTerm = myLog.getTerm(prevIndex)
				entry = myLog.getEntry(index)
				send_message(writer, message.AppendEntries(currentTerm, my_id, prevIndex, prevTerm, entry, myLog.getCommit()))
				print("AppendEntries: term=" + str(currentTerm) + " my_id=" + str(my_id) + " prevIndex=" + str(prevIndex) + " prevTerm=" + str(prevTerm) + " entry=" + str(entry.command.num_tickets) + " commit=" + str(myLog.getCommit()))
				writers.remove(writer)

			for reader in preaders:
				message_in = recieve_message(reader)
				assert type(message_in) is message.AppendEntriesResponse
				if message_in.success:
					readers.remove(reader)
					numVotes = numVotes + 1
				else:
					entry_index[sock_map[reader]] = entry_index[sock_map[reader]] - 1
					writers.append(reader)
		if numVotes >= len(cfg.kiosks)/2:
			success = myLog.setCommit(newLogEntry.index)
			#append_lock.release()
			return message.ClientBuyResponse(myLog.getTickets(), success)
		else:
			#append_lock.release()
			#while myLog.getEntry(newLogEntry.index)
			return message.ClientBuyResponse(myLog.getTickets(), False)
	# if configuration change
	if type(our_message) is message.ClientConfigRequest:
		# phase 1
		config_message = our_message
		cfg_new = config_message.new_config
		cfg_old_new = config.Config(cfg_new.kiosks, cfg_new.delay, cfg.tickets, cfg.kiosks)
		
		success = broadcastAppend(cfg_old_new, cfg_old_new)
		if not success:
			print("Unsuccessful old/new config broadcast")
		
		new_followers = [x for x in cfg_old_new.new_kiosks if x not in cfg_old_new.old_kiosks]
		for f in new_followers:
			if f is not server_addr:
				t = threading.Thread(target=sendHeartbeat, args = (f,))
				t.start()
			
		# phase 2
		success = broadcastAppend(cfg_new, cfg_new)
		if not success:
			print("Unsuccessful new config broadcast")
			
		return message.ClientConfigResponse(success)
			
		newLogEntry = log.LogEntry(currentTerm, myLog.getIndex(), cfg_old_new)
		myLog.appendEntry(newLogEntry)
		our_sockets = [None]*len(cfg_old_new.kiosks)
		readers, writers, errors = [],[],[]
		voters = []
		for x in range(0, len(cfg_old_new.kiosks)):
			if x is not my_id:
				try:
					our_sockets[x] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					our_sockets[x].connect(cfg.kiosks[x])
					our_sockets[x].setblocking(0)
					sock_map[our_sockets[x]] = x
					writers.append(our_sockets[x])
					readers.append(our_sockets[x])
				except:
					pass
		
		
		
		# phase 2
		return
		
	# if client log request
	if type(our_message) is message.ClientLogRequest:
		return message.ClientLogResponse(myLog)
		
	# if requestvote
	if type(our_message) is message.RequestVote:
		vote_message = our_message
		if vote_message.cand_id >= len(myLog.getConfig().kiosks):
			return message.RequestVoteResponse(False, currentTerm)
		if vote_message.term < currentTerm:
			#reply no
			return message.RequestVoteResponse(False, currentTerm)
		if vote_message.term > currentTerm:
			if(debug):
				print("Discovered higher term")
			setTerm(vote_message.term)
			setFollower()
		if vote_message.term >= currentTerm and (votedFor == None or votedFor == vote_message.cand_id) and vote_message.log_term >= myLog.getTerm() and vote_message.log_index >= myLog.getIndex():
			votedFor = vote_message.cand_id
			myState = STATE_FOLLOWER
			resetElectionTimeout()
			return message.RequestVoteResponse(True, currentTerm)
		else:
			return message.RequestVoteResponse(False, currentTerm)
			
	
	# if appendentries
	if type(our_message) is message.AppendEntries:
		append_message = our_message
		if append_message.leader_id >= len(myLog.getConfig().kiosks):
			return None
		#print("AppendEntries: term=" + str(append_message.term) + " my_id=" + str(append_message.leader_id) + " prevIndex=" + str(append_message.prevLogIndex) + " prevTerm=" + str(append_message.prevLogTerm) + " commit=" + str(append_message.commitIndex))
		resetElectionTimeout()
		#if append_message.entries is None:
		#	resetElectionTimeout()
		#	return None
		#if append_message.term < currentTerm:
		#	print("I am higher term than the current leader")
			#setCandidate()
		#	return message.AppendEntriesResponse(True, currentTerm)
		if append_message.term > currentTerm:
			setTerm(append_message.term)
		myState = STATE_FOLLOWER
		log_stack = []
		while True:
			prevIndex = append_message.prevLogIndex
			prevTerm = append_message.prevLogTerm
			nextEntry = append_message.entries
			if nextEntry is not None:
				log_stack.append(nextEntry)
				
			if prevIndex == -1:
				pass
				#log_stack.append(nextEntry)
			elif myLog.getIndex() <= prevIndex or myLog.getTerm(prevIndex) != prevTerm:
				#log_stack.append(nextEntry)
				send_message(our_socket, message.AppendEntriesResponse(False, currentTerm))
				append_message = recieve_message(our_socket)
				assert type(append_message) is message.AppendEntries
				continue
			if prevIndex == -1 or myLog.getTerm(prevIndex) == prevTerm:
				myLog.deleteEntries(prevIndex+1)
				while len(log_stack) > 0:
					myLog.appendEntry(log_stack.pop())
				myLog.setCommit(append_message.commitIndex)
				return message.AppendEntriesResponse(True, currentTerm)
			
		resetElectionTimeout()
		# compare entry with local log
	print("Warning: unhandled message")
	return
	if type(our_message) is message.RequestMessage:
		our_request_message = our_message
		sync_lclock(our_message.lamport_clock)
		pq.put((our_request_message.rank, our_request_message))
		send_message(our_socket, message.ReplyMessage())
		release_message = recieve_message(our_socket)
		assert type(release_message) is message.ReleaseMessage
		update_tickets(release_message.num_tickets)
		with pq_lock:
			pq.get()
		return None
	elif type(our_message) is message.BuyMessage:
		our_buy_message = our_message
		our_sockets = [None]*message.TOTAL_KIOSKS
		readers, writers, errors = [],[],[]
		release_writers = []
		with lclock_lock:
			sync_lclock()
			pq.put((message.get_rank( lclock, get_kiosk_number()),our_buy_message))
			for x in range(0, message.TOTAL_KIOSKS):
				if x is not get_kiosk_number():
					our_sockets[x] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					our_sockets[x].connect(cfg.kiosks[x])
					our_sockets[x].setblocking(0)
					writers.append(our_sockets[x])
					readers.append(our_sockets[x])
					release_writers.append(our_sockets[x])
			while len(writers) != 0:
				_ , pwriters , _ = select.select(readers, writers, errors)
				for writer in pwriters:
					send_message(writer, message.RequestMessage(lclock,get_kiosk_number()))
					writers.remove(writer)
		while len(readers) != 0:
			preaders, _ , _ = select.select(readers, writers, errors)
			for reader in preaders:
				message_in = recieve_message(reader)
				assert type(message_in) is message.ReplyMessage
				readers.remove(reader)
		recvd = False
		while recvd == False:
			with pq_lock:
				our_tuple = pq.get()
				#print("Pulled rank %f off the queue" % our_tuple[0])
				if our_tuple[1] == our_buy_message:
					recvd = True
					success = None
					with ticket_lock:
						if our_buy_message.num_tickets <= tickets:
							success = True
							update_tickets(tickets - our_buy_message.num_tickets)
						else:
							success = False
						while len(release_writers) != 0:
							_, pwriters, _ = select.select([],release_writers, [])
							for writer in pwriters:
								send_message(writer, message.ReleaseMessage(tickets))
								release_writers.remove(writer)
					return message.BuyMessageResponse(success)
				else:
					pq.put(our_tuple)
			time.sleep(float(delay)/2)
	else:
		pass


def main():
	print("Start datacenter")
	global follower_timer
	global my_id
	global myLog
	global append_lock
	my_id = get_kiosk_number()
	follower_timer = threading.Timer(1,setCandidate)
	config_file = sys.argv[2]
	global cfg
	cfg  = config.Config.from_file(config_file)
	#append_lock = [threading.Lock()]*len(cfg.kiosks)
	global tickets
	tickets = cfg.tickets
	global delay
	global server_addr
	myLog = log.Log(tickets, cfg)
	delay = cfg.delay
	message.TOTAL_KIOSKS = len(cfg.kiosks)
	kiosk_number = get_kiosk_number()
	server_addr = cfg.kiosks[kiosk_number]
	num_tickets = cfg.tickets
	server = ThreadedTCPServer(server_addr, ThreadedTCPRequestHandler)
	server_thread = threading.Thread(target = server.serve_forever)
	server_thread.daemon = True
	server_thread.start()
	setFollower()
	while run_server:
		time.sleep(10)
		exit()
		pass

if __name__ == "__main__":
	main()
