import struct
import pickle
from math import floor, ceil
BUY_MESSAGE_CODE = 1
BUY_MESSAGE_RESPONSE_CODE = 2
BUY_SUCCESS = 1
BUY_FAIL = 0
REQUEST_MESSAGE_CODE = 3
TOTAL_KIOSKS = None 
REPLY_MESSAGE_CODE = 4
RELEASE_MESSAGE_CODE = 5
class Message:

	def __init__(self, data):
		self.data = data
		self.data_length  = len(data)

	def serialize(self):
		pass

	@staticmethod
	def deserialize(data):
		return pickle.loads(data)

	@staticmethod
	def serialize(data):
		return pickle.dumps(self)
		
class ClientBuyRequest(Message):

	def toString(self):
		return "Buy " + str(self.num_tickets)

	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, num_tickets):
		self.num_tickets = num_tickets
		super(ClientBuyRequest, self).__init__(self.serialize())
		
class ClientBuyResponse(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, num_tickets, success, leader=None, quorum=None):
		self.num_tickets = num_tickets
		self.success = success
		self.leader = leader
		self.quorum = quorum
		super(ClientBuyResponse, self).__init__(self.serialize())

class ClientLogRequest(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self):
		super(ClientLogRequest, self).__init__(self.serialize())
		
class ClientLogResponse(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, log):
		self.log = log
		super(ClientLogResponse, self).__init__(self.serialize())
				
class ClientConfigRequest(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, new_config):
		self.new_config = new_config
		super(ClientConfigRequest, self).__init__(self.serialize())
		
class ClientConfigResponse(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, success):
		self.success = success
		super(ClientConfigResponse, self).__init__(self.serialize())
		
class RequestVote(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, cand_id, term, log_index, log_term, serverAddr=None):
		self.cand_id = cand_id
		self.term = term
		self.log_index = log_index
		self.log_term = log_term
		self.serverAddr = serverAddr
		super(RequestVote, self).__init__(self.serialize())
		
class RequestVoteResponse(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, voteGranted, term):
		self.voteGranted = voteGranted
		self.term = term
		super(RequestVoteResponse, self).__init__(self.serialize())
		
class AppendEntries(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, term, leader_id, prevLogIndex, prevLogTerm, entries, commitIndex, serverAddr=None):
		self.term = term
		self.leader_id = leader_id
		self.prevLogIndex = prevLogIndex
		self.prevLogTerm = prevLogTerm
		self.entries = entries
		self.commitIndex = commitIndex
		self.serverAddr = serverAddr
		super(AppendEntries, self).__init__(self.serialize())
		
class AppendEntriesResponse(Message):
	def serialize(self):
		return pickle.dumps(self)

	def __init__(self, success, term):
		self.success = success
		self.term = term
		super(AppendEntriesResponse, self).__init__(self.serialize())