import message
import config
import threading


#log_lock = threading.Lock()
class LogEntry:

	def __init__(self, term, index, command, commit=False):
		self.term = term
		self.index = index
		self.command = command
		self.committed = commit

class Log:

	def __init__(self, numtickets, cfg=None):
		self.entries = []
		self.commitIndex = -1
		self.num_tickets = numtickets
		self.config = cfg
		self.followerIndices = {}
		return
		
	def printLog(self):
		print("------------------------------")
		for index in range(len(self.entries)):
			e = self.entries[index]
			if index <= self.commitIndex:
				print("Index: " + str(e.index) + " term: " + str(e.term) + " command: " + e.command.toString() + " committed: True")
			else:
				print("Index: " + str(e.index) + " term: " + str(e.term) + " command: " + e.command.toString() + " committed: False")
		print("------------------------------")
		print("Remaining tickets: " + str(self.getTickets()))
		print("Old kiosks: " + str(self.config.old_kiosks))
		print("New kiosks: " + str(self.config.new_kiosks))
		
	def clearFollowerIndices(self):
		self.followerIndices = {}
		return
		
	def setFollowerIndex(self, f, i):
		self.followerIndices[f] = i
		return
		
	def checkFollowerIndices(self, myAddr, leader=False, currentTerm=None):
		#log_lock.acquire()
		if self.commitIndex >= self.getIndex()-1:
			#log_lock.release()
			return
		voters = [myAddr]
		for f in self.followerIndices:
			if self.followerIndices[f] > self.commitIndex:
				voters.append(f)
		if self.getConfig().hasQuorum(voters):
			self.incrCommit(leader, currentTerm)
		#log_lock.release()
		
	def getConfig(self):
		return self.config
		
	def getTickets(self):
		return self.num_tickets
	# delete all entries after index
	def deleteEntries(self, index):
		if index < 0:
			index = 0
		del self.entries[index:]
		
	def appendEntry(self, entry):
		#log_lock.acquire()
		if type(entry.command) is config.Config:
			print("Changing config")
			self.config = entry.command
			print(str(self.config.kiosks))
		self.entries.append(entry)
		#log_lock.release()
		return

	def getEntry(self, index):
		return self.entries[index]
		
	# returns term of last entry	
	def getTerm(self, index = None):
		if(len(self.entries) == 0):
			return -1
		if index is None:
			return self.entries[-1].term
		elif index == -1:
			return -1
		else:
			try:
				return self.entries[index].term
			except IndexError:
				print("My length: " + str(len(self.entries)) + " index=" + str(index))
				return -1
	def incrCommit(self, leader=False, currentTerm=None):
		self.commitIndex = self.commitIndex + 1
		entry = self.getEntry(self.commitIndex)
		entry.committed = True
		if type(entry.command) is message.ClientBuyRequest:
			tix = entry.command.num_tickets
			if self.num_tickets - tix >= 0:
				self.num_tickets = self.num_tickets - tix
				return True
			else:
				return False
		elif type(entry.command) is config.Config and leader and entry.command.old_kiosks is not None:
			print("Appending Cnew")
			Cold_new = entry.command
			Cnew = config.Config(Cold_new.new_kiosks, Cold_new.delay, self.num_tickets)
			Cnew_log = LogEntry(currentTerm, self.getIndex(), Cnew)
			self.appendEntry(Cnew_log)
			return True
		else:
			pass
			#print("Unexpected command")
	def setCommit(self, c, leader=False, currentTerm=None):
		#log_lock.acquire()
		if self.getTerm() != currentTerm:
			return False
		initial = self.commitIndex
		final = c
		val = False
		if initial >= final:
			#log_lock.release()
			return True
		for ind in range(initial, final):
			val = self.incrCommit(leader, currentTerm)
		self.commitIndex = c
		#log_lock.release()
		return val
	# returns index of last committed entry
	def getCommit(self):
		x = 0
		while x < len(self.entries):
			if self.entries[x].committed == False:
				break
			x = x + 1
		return x - 1
		
	def getIndex(self):
		return len(self.entries)
	
	def deleteEntry(self, index):
		
		return