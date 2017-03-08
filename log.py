

class LogEntry:

	def __init__(self, term, index, command):
		self.term = term
		self.index = index
		self.command = command

class Log:

	def __init__(self):
		self.entries = []
		return

	def appendEntry(self, entry):
		self.entries.append(entry)
		return
	
	def getTerm():
		return
		
	def getIndex():
		return
	
	def popEntry(self):
		return