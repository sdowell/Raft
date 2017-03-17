class Kiosk:
	def __init__(self, id, hostname, port):
		self.id = id
		self.hostname = hostname
		self.port = port
		
	def getAddress(self):
		return (self.hostname, self.port)
		
	def getId(self):
		return self.id


class Config:

	@staticmethod 
	def from_file(filename):
		kiosks, delay, numtickets = [], None, None
		with open(filename) as cfile:
			for line in cfile.readlines():
				spl = line.split()
				if spl[0] == 'kiosk':
					if len(spl) == 2:
						kiosks.append(('localhost', int(spl[1])))
					elif len(spl) == 3:
						kiosks.append(spl[1], int(spl[2]))
				elif spl[0] == "tickets":
					numtickets = int(spl[1])
				elif spl[0] == "delay":
					delay = int(spl[1])
			if len(kiosks) == 0:
				print("Error: no kiosks found")
			if delay is None:
				print("Warning: no delay declared in cfg file, assuming 0.")
				delay = 0
			if numtickets == None:
				pass #Do something later?
		return Config(kiosks, delay, tickets = numtickets)

	def to_file(filename):
		pass

	def toString(self):
		if self.old_kiosks is None:
			return "Cnew: " + str(self.new_kiosks)
		else:
			return "Cold+Cnew: " + str(self.old_kiosks) + " -> " + str(self.new_kiosks)
		
	# param voters: list of address tuples for voters
	def hasQuorum(self, voters):
		if self.old_kiosks is not None:
			#old_ids = [x.getId() for x in self.old_kiosks]
			old_votes = 0
			for v in voters:
				if v in self.old_kiosks:
					old_votes = old_votes + 1
			if old_votes < len(self.old_kiosks)/2:
				return False
		new_votes = 0
		for v in voters:
			if v in self.new_kiosks:
				new_votes = new_votes + 1
		if new_votes < len(self.new_kiosks)/2:
			return False
		else:
			return True
	
	def __init__(self, new_kiosks, delay, tickets = None, old_kiosks = None):
		self.old_kiosks = old_kiosks
		self.new_kiosks = new_kiosks
		self.kiosks = []
		if self.old_kiosks is not None:
			for ok in self.old_kiosks:
				self.kiosks.append(ok)
		for nk in self.new_kiosks:
			if nk not in self.kiosks:
				self.kiosks.append(nk)
		self.delay = delay
		self.tickets = tickets
