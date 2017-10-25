import zmq
import math
from datetime import datetime
from time import sleep
import os


# Function to send commands to ZeroMQ MT4 EA
def remote_send(socket, data):

    msg = None
    try:

        socket.send(data)
        msg = socket.recv_string()

    except zmq.Again as e:
        print ("1.Waiting for PUSH from MetaTrader 4.. :", e)
        sleep(1)


# Function to retrieve data from ZeroMQ MT4 EA
def remote_pull(socket):

    msg = None
    try:
        msg = socket.recv()
        # msg = socket.recv(flags=zmq.NOBLOCK)

    except zmq.Again as e:
        print ("2.Waiting for PUSH from MetaTrader 4.. :", e)
        sleep(3)

    return msg



class broker_class:
    tms = None
    ask = 0.0
    bid = 0.0
    spread = 0.0
    digits = 0
    avg_price = 0.0
    trade_count = None
    err_msg = None
    req_socket = None
    pull_socket = None
    symbol = None
    magic_number = None

    def __init__(self, broker, pair, magic_no):
        self.get_socket(broker)
        self.symbol = pair
        self.magic_number = magic_no


    def get_socket(self, broker):
        context = zmq.Context()

        socket_req = broker + ':5555'
        socket_pull = broker + ':5556'

        # Create REQ Socket
        reqSocket = context.socket(zmq.REQ)
        reqSocket.connect(socket_req)
        self.req_socket = reqSocket

        # Create PULL Socket
        pullSocket = context.socket(zmq.PULL)
        pullSocket.connect(socket_pull)
        self.pull_socket = pullSocket



    def get_price(self):

        get_rates = "RATES|"+self.symbol


        remote_send(self.req_socket, get_rates)
        msg = remote_pull(self.pull_socket)

        # print msg
        if msg is not None:
            # print msg
            quote = msg.split('|')
            # print quote
            self.tms =  datetime.strptime(quote[0], '%Y.%m.%d %H:%M:%S')
            self.bid = float(quote[1])
            self.ask = float(quote[2])
            self.spread = float(quote[3])
            self.digits = int(float(quote[4]))
            self.avg_price = (self.bid + self.ask)/2

        # print self.bid, self.ask, self.avg_price

    def get_count(self):

        req_count = "COUNT|"+ self.symbol+"|"+ str(self.magic_number)

        remote_send(self.req_socket, req_count)
        msg = remote_pull(self.pull_socket)
        # print msg

        if msg is not None:
            quote = msg.split('|')
            self.trade_count = int(float(quote[0]))


    def send_order(self, order_type, price, lot=0.01, slip=10, comments="no comments"):

        #format 'TRADE|OPEN|ordertype|symbol|openprice|lot|SL|TP|Slip|comments|magicnumber'

        order = "TRADE|OPEN|"+ str(order_type)+"|" + self.symbol +"|"+ str(price)+"|"+ str(lot)+"|0|0|" + str(slip)+"|"+comments +"|"+str(self.magic_number)

        remote_send(self.req_socket, order)
        msg = remote_pull(self.pull_socket)
        # print  msg
        if msg is not None:
            quote = msg.split('|')
            self.err_msg = bool(int(quote[0]))

    def order_close(self):

        # format 'TRADE|CLOSE|symbol|magicnumber'
        close_order = 'TRADE|CLOSE|'+ str(self.symbol) +'|'+ str(self.magic_number)

        remote_send(self.req_socket, close_order)
        msg = remote_pull(self.pull_socket)

        if msg is not None:
            quote = msg.split('|')
            self.err_msg = bool(int(quote[0]))

def write_2_file(data, filename):
    file = open(filename, 'w')
    file.write(data)

    file.close()


# Run Tests -----------------------------------------------------------
print 'PROGRAM START'

get_broker = "COMPANY"

ip_1 = 'tcp://127.0.0.99'
ip_2 = 'tcp://127.0.0.100'
magic_number = 751229

arbitrage_limit = 30

total_spread_limit = 60
open_pip = 0.0
arb_limit_norm = 0.0

#------------------------ Read Log File if exist -------------
logfilename = 'OPEN_PIP.log'

if os.path.exists(logfilename) :

    file = open(logfilename, 'r')
    data = file.read()
    open_pip = float(data)

    file.close()

#-----------------------------------------------
#get bid and ask from broker
broker1 = broker_class(ip_1, 'GBPUSD.m', magic_number)
broker2 = broker_class(ip_2, 'GBPUSD', magic_number)

print  'open pip is :', open_pip

while True:
    try:
        while True:

            pip_diff = 0.0
            total_spread = 0.0

            broker1.get_price()
            broker2.get_price()
            broker1.get_count()
            broker2.get_count()

            if broker1.tms == broker2.tms and broker1.bid != 0 and broker2.bid != 0:
                pip_diff = round((broker1.avg_price - broker2.avg_price) * math.pow(10, broker1.digits),1)
                total_spread = broker1.spread + broker2.spread
                arb_limit_norm = arbitrage_limit / math.pow(10, broker1.digits)
                # print 'Arb limit Norm=', arb_limit_norm

                # if pip_diff >= arbitrage_limit or pip_diff <= - arbitrage_limit:
            print 'timestamp1:', broker1.tms, 'timestamp2:', broker2.tms,'\t pip diff=', pip_diff, '\t total spread=', total_spread

            '''
            Conclusion
            
            OPen Position :
            if broker1.ask <= (broker2.bid - 3pip) and pip_diff <0 (-ve)
                then Buy A, Sell B
                            
            if broker1.bid >= (broker2.ask + 3 pip) and  pip_diff > 0 (+ve)
                then Sell A, Buy B
                                
            close Position:
            if open_pip_diff < 0 (-ve)
                if broker1.bid > (broker2.ask + 3 pip)
                
            if open_pip_diff > 0 (+ve)
                if broker1.ask <= (broker2.bid - 3 pip)
            
            '''

            if broker1.trade_count == 0 and broker2.trade_count == 0:
                if pip_diff > 0 and  broker1.bid >= (broker2.ask + arb_limit_norm):

                    broker1.send_order(1, broker1.bid, 0.01, 10, 'greater arb') # JustForex
                    broker2.send_order(0, broker2.ask, 0.10, 10, 'greater arb') # SuperForex
                    open_pip = pip_diff
                    write_2_file(str(open_pip), logfilename)
                    print 'broker 1: Open status=', broker1.err_msg
                    print 'broker 2: Open status=', broker2.err_msg

                elif pip_diff < 0 and  broker1.ask <= (broker2.bid - arb_limit_norm):

                    broker1.send_order(0, broker1.ask, 0.01, 10, 'smaller arb') # JustForex
                    broker2.send_order(1, broker2.bid, 0.10, 10, 'smaller arb') #SuperForex
                    open_pip = pip_diff
                    write_2_file(str(open_pip), logfilename)
                    print 'broker 1: Open status=', broker1.err_msg
                    print 'broker 2: Open status=', broker2.err_msg

            elif broker1.trade_count > 0 and broker2.trade_count > 0:
                if open_pip > 0 and  broker1.ask <= (broker2.bid - arb_limit_norm):

                    broker1.order_close()
                    broker2.order_close()
                    open_pip = 0.0
                    os.remove(logfilename)
                    print 'broker 1: Close status=', broker1.err_msg
                    print 'broker 2: Close status=', broker2.err_msg

                if open_pip < 0 and  broker1.bid >= (broker2.ask + arb_limit_norm):

                    broker1.order_close()
                    broker2.order_close()
                    open_pip = 0.0
                    os.remove(logfilename)
                    print 'broker 1: Close status=', broker1.err_msg
                    print 'broker 2: Close status=', broker2.err_msg

    except:
        print("retrying due to error ...")
        broker1 = broker_class(ip_1, 'GBPUSD_', magic_number)
        broker2 = broker_class(ip_2, 'GBPUSDh', magic_number)
        sleep(1.0)
        continue

    break
