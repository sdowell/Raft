import message
import config

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
		self.entries.append(entry)
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
	def incrCommit(self):
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
		elif type(entry.command) is config.Config:
			print("Changing config")
			self.config = entry.command
			return True
		else:
			print("Unexpected command")
	def setCommit(self, c):
		initial = self.commitIndex
		final = c
		val = False
		for ind in range(initial, final):
			val = self.incrCommit()
		self.commitIndex = c
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