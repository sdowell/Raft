import socket
import threading
import socketserver
import sys
import queue
import select
import time
import message
import config 

run_server = True
#server priority queue
pq = queue.PriorityQueue()
pq_lock = threading.RLock()
lclock = 0
lclock_lock = threading.RLock()
ticket_lock = threading.RLock()
cfg = None
tickets = None
delay = None
class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
	
	def handle(self):
		message_in = recieve_message(self.request)
		cur_thread = threading.current_thread()
		response_message = handle_message(message_in, self.request)
		send_message(self.request, response_message)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
	
	def __exit__(self):
		self.shutdown()

def recieve_message(a_socket):
	m_in = message.Message.deserialize(a_socket.recv(5))
	time.sleep(delay)
	print("Recieved message of type: %s from %s" % (str(type(m_in)), str(a_socket.getpeername())))
	return m_in

def send_message(a_socket, m_out = None):
	if m_out is not None:
		a_socket.send(m_out.serialize())
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

def sync_lclock(clock_val = None):
	global lclock
	with lclock_lock:
		if clock_val is not None and clock_val >= lclock:
			lclock = clock_val + 1
		else:
			lclock = lclock + 1
		print("Updated lamport_clock, new value: %d" % lclock)

def handle_message(our_message, our_socket):
	global tickets
	# if client message
		# if buy request
		
		# if configuration change
		
	# if requestvote
	
	# if appendentries
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
	global cfg
	cfg  = config.Config.from_file("config.txt")
	global tickets
	tickets = cfg.tickets
	global delay
	delay = cfg.delay
	message.TOTAL_KIOSKS = len(cfg.kiosks)
	kiosk_number = get_kiosk_number()
	server_addr = cfg.kiosks[kiosk_number]
	num_tickets = cfg.tickets
	server = ThreadedTCPServer(server_addr, ThreadedTCPRequestHandler)
	server_thread = threading.Thread(target = server.serve_forever)
	server_thread.daemon = True
	server_thread.start()
	while run_server:
		pass

if __name__ == "__main__":
	main()
