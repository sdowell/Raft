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

	def __init__(self, kiosks, delay, tickets = None, old_kiosks = None):
		self.old_kiosks = old_kiosks
		self.kiosks = kiosks
		self.delay = delay
		self.tickets = tickets
