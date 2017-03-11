import socket
import config
import message
import time
import sys
delay = None

def requestTickets(kiosk, tickets):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	print("Attempting to connect to port: " + str(kiosk))
	s.connect((str(kiosk[0]), int(kiosk[1])))
	buy_message = message.ClientBuyRequest(tickets)
	s.send(buy_message.data)
	response = s.recv(4096)
	#time.sleep(delay)
	success = message.Message.deserialize(response)
	s.close()
	return success

def buyTickets(cfg, myKiosk):
	#ask user for number of tickets
	buytickets = input("How many tickets would you like to purchase? ")
	try:
		buytickets = int(buytickets)
	except ValueError:
		print("Invalid input")
		return
	#request to purchase tickets from selected kiosk
	response = requestTickets(cfg.kiosks[myKiosk], buytickets)
	if response.success == True:
		print("Tickets purchased successfully")
	elif response.success == False:
		print("Tickets not purchased")
	else:
		print("Error: unrecognized response")	


		
def showLog(cfg, myKiosk):
	kiosk = cfg.kiosks[myKiosk]
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	print("Attempting to connect to port: " + str(kiosk))
	s.connect((str(kiosk[0]), int(kiosk[1])))
	log_message = message.ClientLogRequest()
	s.send(log_message.data)
	response = s.recv(4096)
	#time.sleep(delay)
	r_obj = message.Message.deserialize(response)
	s.close()
	r_obj.log.printLog()
	return r_obj.log

def changeConfig(cfg, myKiosk):
	fname = input("Enter the name of the configuration file ")
	try:
		fname = str(fname)
	except ValueError:
		print("Invalid input")
		return
	#request to purchase tickets from selected kiosk
	kiosk = cfg.kiosks[myKiosk]
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	print("Attempting to connect to port: " + str(kiosk))
	s.connect((str(kiosk[0]), int(kiosk[1])))
	new_config = config.Config.from_file(fname)
	cfg_message = message.ClientConfigRequest(new_config)
	s.send(cfg_message.data)
	response = s.recv(4096)
	#time.sleep(delay)
	r_obj = message.Message.deserialize(response)
	s.close()
	if r_obj.success == True:
		print("Config changed successfully")
	elif r_obj.success == False:
		print("Config not changed successfully")
	else:
		print("Error: unrecognized response")
	return new_config
	
def cmdUI(cfg):
	done = False
	while not done:
		#print information
		i = 0
		for k in cfg.kiosks:
			print(str(i) + ". " + str(k))
			i = i + 1
		print(str(i) + ". exit")
		#ask user for kiosk to connect to
		try:
			a_input = input("Please choose a kiosk or exit: ")
		except NameError:
			print("Invalid input: expected int")
			continue
		try:
			a_input = int(a_input)
		except ValueError:
			print("Invalid input")
			continue
		if a_input < 0 or a_input > i:
			print("Input out of range")
			continue
		if a_input == i:
			done = True
			break
		myKiosk = a_input
		print("1. Buy Tickets")
		print("2. Show Log")
		print("3. Change Configuration")
		try:
			a_input = input("Please choose to either buy tickets or view the log: ")
		except NameError:
			print("Invalid input: expected int")
			continue		
		try:
			a_input = int(a_input)
		except ValueError:
			print("Invalid input")
			continue
		if a_input == 1:
			buyTickets(cfg, myKiosk)
			continue
		elif a_input == 2:
			showLog(cfg, myKiosk)
			continue
		elif a_input == 3:
			cfg = changeConfig(cfg, myKiosk)
			continue
		else:
			print("Unexpected input")
			continue
		

def main():
	print("Starting client...")
	cfgfile = sys.argv[1]
	cfg = config.Config.from_file(cfgfile)
	global delay
	delay = cfg.delay
	cmdUI(cfg)
	exit()


if __name__ == "__main__":
	main()
