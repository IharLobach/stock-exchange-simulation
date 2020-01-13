import threading
from threading import Thread
import socket
import sys
import time
from server import *
import json
import random
import select


HOST, PORT = "127.0.0.1", 9999

class Trader(Thread):
    def __init__(self,lock, id):
        super().__init__()
        self.id = id
        self.lock = lock
        self.book_position = 0
        self.balance_track = [1000000]
        self.owned_positions = [1000000]

    def place_limit_order(self, quantity=None, price=None, side=None):
        return (ActionType.PLACE,self.id,LimitOrder(self.id,"AAPL",quantity,price,side,time.time()))

    def place_market_order(self, quantity=None, side=None):
        return (ActionType.PLACE,self.id,MarketOrder(self.id,"AAPL",quantity,side,time.time()))

    def place_ioc_order(self, quantity=None, price=None, side=None):
        return (ActionType.PLACE,self.id,IOCOrder(self.id,"AAPL",quantity,price,side,time.time()))

    def amend_quantity(self, quantity=None):
        return (ActionType.AMEND,self.id,quantity)

    def cancel_order(self):
        return (ActionType.CANCEL,self.id)

    def balance_and_position(self):
        return (ActionType.BALANCE,self.id)

    def convert_tuple_to_dic_action(self, t):
        dic = {"ActionType":t[0].value,"TraderID":t[1]}
        if t[0]==ActionType.PLACE:
            o = t[2]
            dic["OrderType"]=o.type.value
            dic["Symbol"] = o.symbol
            dic["Quantity"] = o.quantity
            dic["Side"] = o.side.value
            dic["Time"] = o.time
            if not isinstance(o,MarketOrder):
                dic["Price"] = o.price
        elif t[0]==ActionType.AMEND:
            dic["Quantity"] = t[2]
        elif t[0]==ActionType.CANCEL:
            pass
        elif t[0]==ActionType.BALANCE:
            pass


        return dic

    def convert_dic_from_exchange_to_tuple(self,dic):
        actionType = ActionType(dic["ActionType"])
        if actionType==ActionType.PLACE:
            isPresent = bool(dic["OrderPresent"])
            trader_id = dic["TraderID"]
            if dic["IsFilledOrder"]:
                o = FilledOrder(trader_id,dic["Symbol"],dic["Quantity"],dic["Price"],OrderSide(dic["Side"]),dic["Time"])
            else:
                orderType = OrderType(dic["OrderType"])
                if orderType == OrderType.LIMIT:
                    o = LimitOrder(trader_id, dic["Symbol"], dic["Quantity"], dic["Price"], OrderSide(dic["Side"]), dic["Time"])
                elif orderType == OrderType.MARKET:
                    o = MarketOrder(trader_id, dic["Symbol"], dic["Quantity"], OrderSide(dic["Side"]), dic["Time"])
                elif orderType == OrderType.IOC:
                    o = IOCOrder(trader_id, dic["Symbol"], dic["Price"], dic["Quantity"], OrderSide(dic["Side"]), dic["Time"])
            return (actionType, o,isPresent)
        elif actionType == ActionType.AMEND:
            return (actionType, bool(dic["Successfully"]), dic["Quantity"])
        elif actionType == ActionType.CANCEL:
            return (actionType, bool(dic["Successfully"]))
        elif actionType == ActionType.BALANCE:
            return (actionType,(dic["Balance"],dic["Position"],dic["BookPosition"]))
        else:
            raise UndefinedTraderAction

    def process_response(self, response):
        # Implement this function
        # You need to process each order according to the type (by enum) given by the 'response' variable
        # If the action taken by the trader is ambiguous you need to raise the following error
        actionType = response[0]
        if actionType not in ActionType:
            raise UndefinedResponse("Undefined Response Received!")
        elif actionType==ActionType.PLACE:
            orderPresent = response[2]
            if not orderPresent:
                o = response[1]
                if isinstance(o,FilledOrder):
                    pos_delta = o.quantity if o.side==OrderSide.BUY else -o.quantity
                    bal_delta = -o.price*pos_delta
                    self.balance_track.append(self.balance_track[-1]+bal_delta)
                    self.owned_positions.append(self.owned_positions[-1]+pos_delta)
                    if o.limit:
                        self.book_position-=o.quantity
                elif isinstance(o,LimitOrder):
                    self.book_position+=o.quantity
            else: # when the match engine tried to place this order it turned out that there was already an order from
                # this trader on the book. Each trader can only have one order on the book at a time.
                #print("Trader {} already has order on the books. Thus, the new order was rejected.".format(self.id))
                pass
        elif actionType==ActionType.AMEND:
            amended_successfully = response[1]
            if amended_successfully:
                quantity = response[2]
                self.book_position = quantity
        elif actionType==ActionType.CANCEL:
            cancelled_successfully = response[1]
            if cancelled_successfully:
                self.book_position = 0
        else: # actionType==ActionType.BALANCE:
            # it's weird that we have two ways to update balance and book position
            # I will not use this method (updating by values from the matching engine) to avoid confusion
            balance = response[1][0]
            self.owned_positions = response[1][1]
            self.book_position = response[1][2]

    def random_action(self):

        # Implement this function
        # According to the status of whether you have a position on the book and the action chosen
        # the trader needs to be able to take a separate action
        # The action taken can be random or deterministic, your choice
        if self.book_position>0:
            # 1/10 chance to cancel, 1/10 chance to amend, 8/10 chance to just wait
            i = random.randint(0,10)
            if i==0 or self.book_position==500:
                return self.cancel_order()
            elif i==1:
                return self.amend_quantity(self.book_position-500)
            else:
                return None
        else:
            s = random.randint(0, 1)
            side = OrderSide.BUY if s else OrderSide.SELL
            orderType = None
            ot = random.randint(0,2)
            if ot==0:
                orderType = OrderType.LIMIT
            elif ot==1:
                orderType = OrderType.MARKET
            else: #ot==2
                orderType = OrderType.IOC
            if side==OrderSide.BUY:
                quantity = 500#  + random.randint(-10,10)# random.randint(1,1000)
                bal = self.balance_track[-1]
                price = 1000# + random.randint(-10,10)
                if orderType == OrderType.LIMIT:
                    #self.book_position = quantity
                    return self.place_limit_order(quantity, price, side)
                elif orderType == OrderType.MARKET:
                    return self.place_market_order(quantity, side)
                else:  # orderType==OrderType.IOC
                    return self.place_ioc_order(quantity, price, side)
            else: #side = OrderSide.SELL
                quantity = 500# random.randint(1,1000)
                price = 1000# + random.randint(-10,10)# random.uniform(10/quantity,1000000/quantity)
                if orderType==OrderType.LIMIT:
                    #self.book_position = quantity
                    return self.place_limit_order(quantity, price, side)
                elif orderType==OrderType.MARKET:
                    return self.place_market_order(quantity,side)
                else: # orderType==OrderType.IOC
                    return self.place_ioc_order(quantity,price,side)
            # i = random.randint(0, 2)
            # s = random.randint(0,1)
            # side = OrderSide.BUY if s else OrderSide.SELL
            # if i==0:
            #     return self.place_limit_order(random.randint(20,100),random.uniform(200,400),side)
            # elif i==1:
            #     return self.place_market_order(random.randint(20,100),side)
            # elif i==2:
            #     return self.place_ioc_order(random.randint(20,100),random.uniform(200,400),side)
            # else:
            #     raise Exception()

    def run(self):
        # Create a socket (SOCK_STREAM means a TCP socket)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        sock.bind(("127.0.0.{}".format(self.id+2), 9999))
        # Connect to server and send data
        sock.connect((HOST, PORT))
        send_fixed_len(sock,str(self.id))
        received = recv_fixed_len(sock)
        self.lock.acquire()
        print("my id was accepted by the server. It's {}".format(received))
        self.lock.release()
        i = 0
        time.sleep(5)
        sock.settimeout(1)
        while True:
            #time.sleep(0.5)
            if self.balance_track[-1]>0:
                ra = self.random_action()
                if ra is not None:
                    to_send = json.dumps(self.convert_tuple_to_dic_action(ra))
                    send_fixed_len(sock,to_send)
                    self.lock.acquire()
                    print("Trader with id = {} Sent:     {}".format(self.id, to_send))
                    self.lock.release()
            while True:
                try:
                    received = recv_fixed_len(sock)
                except socket.timeout as e:
                    err = e.args[0]
                    if err == 'timed out':
                        break
                self.lock.acquire()
                print("Trader with id = {} Received: {}".format(self.id, received))
                self.lock.release()
                self.process_response(self.convert_dic_from_exchange_to_tuple(json.loads(received)))
                i += 1
            time.sleep(1)
        self.lock.acquire()
        print("trader with id =  {} is closing the connection".format(self.id))
        self.lock.release()
        sock.close()




if __name__=="__main__":
    lock = threading.Lock()
    n_tr = 100
    traders = [Trader(lock,i) for i in range(n_tr)]
    for t in traders:
        t.start()
    for t in traders:
        t.join()







