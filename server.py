
import socket
import threading
from threading import Thread
import socketserver
import time
from collections import deque
import json
from enum import Enum

trader_sockets = {}
exchange_lock = threading.Lock()
trader_sockets_lock = threading.Lock()
print_lock = threading.Lock()

class OrderType(Enum):
    LIMIT = 1
    MARKET = 2
    IOC = 3


class OrderSide(Enum):
    BUY = 1
    SELL = 2


class NonPositiveQuantity(Exception):
    pass


class NonPositivePrice(Exception):
    pass


class InvalidSide(Exception):
    pass


class UndefinedOrderType(Exception):
    pass


class UndefinedOrderSide(Exception):
    pass


class NewQuantityNotSmaller(Exception):
    pass



class UndefinedTraderAction(Exception):
    pass


class UndefinedResponse(Exception):
    pass


from abc import ABC


class Order(ABC):
    def __init__(self, id, symbol, quantity, side, time):
        self.id = id
        self.symbol = symbol
        if quantity > 0:
            self.quantity = quantity
        else:
            raise NonPositiveQuantity("Quantity Must Be Positive!")
        if side in [OrderSide.BUY, OrderSide.SELL]:
            self.side = side
        else:
            raise InvalidSide(
                "Side Must Be Either \"Buy\" or \"OrderSide.SELL\"!")
        self.time = time


class LimitOrder(Order):
    def __init__(self, id, symbol, quantity, price, side, time):
        super().__init__(id, symbol, quantity, side, time)
        if price > 0:
            self.price = price
        else:
            raise NonPositivePrice("Price Must Be Positive!")
        self.type = OrderType.LIMIT



class MarketOrder(Order):
    def __init__(self, id, symbol, quantity, side, time):
        super().__init__(id, symbol, quantity, side, time)
        self.type = OrderType.MARKET


class IOCOrder(Order):
    def __init__(self, id, symbol, quantity, price, side, time):
        super().__init__(id, symbol, quantity, side, time)
        if price > 0:
            self.price = price
        else:
            raise NonPositivePrice("Price Must Be Positive!")
        self.type = OrderType.IOC

class FilledOrder(Order):
    def __init__(self, id, symbol, quantity, price, side, time, limit=False):
        super().__init__(id, symbol, quantity, side, time)
        if price > 0:
            self.price = price
        else:
            raise NonPositivePrice("Price Must Be Positive!")
        self.limit = limit



class NoOrderWithThisIDInOrderBook(Exception):
    pass


class MatchingEngine():
    def __init__(self):
        self.bid_book = []
        self.ask_book = []
        # These are the order books you are given and expected to use for matching the orders below

    # Note: As you implement the following functions keep in mind that these enums are available:
    #     class OrderType(Enum):
    #         LIMIT = 1
    #         MARKET = 2
    #         IOC = 3

    #     class OrderSide(Enum):
    #         BUY = 1
    #         SELL = 2

    def handle_order(self, order):
        # Implement this function
        # In this function you need to call different functions from the matching engine
        # depending on the type of order you are given
        if order.type == OrderType.LIMIT:
            return self.handle_limit_order(order)
        elif order.type == OrderType.MARKET:
            return self.handle_market_order(order)
        elif order.type == OrderType.IOC:
            return self.handle_ioc_order(order)
        else:
            # You need to raise the following error if the type of order is ambiguous
            raise UndefinedOrderType("Undefined Order Type!")

    def handle_transaction(self,filled_orders,o_book,order,quantity,price):
        filled_orders.append(FilledOrder(o_book.id, o_book.symbol, quantity, price, o_book.side, time.time(), o_book.type == OrderType.LIMIT))
        filled_orders.append(FilledOrder(order.id, order.symbol, quantity, price, order.side,
                                         time.time(), order.type == OrderType.LIMIT))


    def handle_limit_order(self, order):
        if order.side not in OrderSide:
            raise UndefinedOrderSide("Undefined Order Side!")
        # Implement this function
        # Keep in mind what happens to the orders in the limit order books when orders get filled
        # or if there are no crosses from this order
        # in other words, handle_limit_order accepts an arbitrary limit order that can either be
        # filled if the limit order price crosses the book, or placed in the book. If the latter,
        # pass the order to insert_limit_order below.
        filled_orders = []
        # The orders that are filled from the market order need to be inserted into the above list
        order_side_is_sell = order.side == OrderSide.SELL
        book = self.bid_book if order_side_is_sell else self.ask_book
        while order.quantity and len(book):
            o = book[0]
            if (o.price < order.price) == order_side_is_sell:
                break
            elif o.quantity > order.quantity:
                self.handle_transaction(filled_orders,o,order,order.quantity,o.price)
                o.quantity-=order.quantity
                break
            else: #order_from_book.quantity <= order.quantity
                self.handle_transaction(filled_orders,o,order,o.quantity,o.price)
                order.quantity -= o.quantity
                book.remove(o)

        if order.quantity:
            self.insert_limit_order(order)
        return filled_orders

    def handle_market_order(self, order):
        if order.side not in OrderSide:
            raise UndefinedOrderSide("Undefined Order Side!")
        # Implement this function
        # Keep in mind what happens to the orders in the limit order books when orders get filled
        # or if there are no crosses from this order
        # in other words, handle_limit_order accepts an arbitrary limit order that can either be
        # filled if the limit order price crosses the book, or placed in the book. If the latter,
        # pass the order to insert_limit_order below.
        filled_orders = []
        # The orders that are filled from the market order need to be inserted into the above list
        order_side_is_sell = order.side == OrderSide.SELL
        book = self.bid_book if order_side_is_sell else self.ask_book
        while order.quantity and len(book):
            o = book[0]
            if o.quantity > order.quantity:
                self.handle_transaction(filled_orders, o, order, order.quantity, o.price)
                o.quantity-=order.quantity
                break
            else: #order_from_book.quantity < order.quantity
                self.handle_transaction(filled_orders, o, order, o.quantity, o.price)
                order.quantity -= o.quantity
                book.remove(o)

        return filled_orders

    def handle_ioc_order(self, order):
        if order.side not in OrderSide:
            raise UndefinedOrderSide("Undefined Order Side!")
        # Implement this function
        filled_orders = []
        # The orders that are filled from the ioc order need to be inserted into the above list
        order_side_is_sell = order.side == OrderSide.SELL
        book = self.bid_book if order_side_is_sell else self.ask_book
        while order.quantity and len(book):
            o = book[0]
            if (o.price < order.price) == order_side_is_sell:
                break
            elif o.quantity > order.quantity:
                self.handle_transaction(filled_orders, o, order, order.quantity, o.price)
                o.quantity-=order.quantity
                break
            else: #order_from_book.quantity < order.quantity
                self.handle_transaction(filled_orders, o, order, o.quantity, o.price)
                order.quantity -= o.quantity
                book.remove(o)
        # The filled orders are expected to be the return variable (list)
        return filled_orders

    def insert_limit_order(self, order):
        assert order.type == OrderType.LIMIT
        # Implement this function
        # this function's sole puporse is to place limit orders in the book that are guaranteed
        # to not immediately fill
        if order.side not in OrderSide:
            # You need to raise the following error if the side the order is for is ambiguous
            raise UndefinedOrderSide("Undefined Order Side!")
        order_side_is_sell = order.side == OrderSide.SELL
        book = self.ask_book if order_side_is_sell else self.bid_book
        sign = 1 if order_side_is_sell else -1
        i = 0
        for o in book:
            if (sign*o.price, o.time)>(sign*order.price,order.time):
                break
            i+=1
        book.insert(i,order)

    def find_order(self,id):
        for o in self.ask_book:
            if o.id==id:
                return o
        for o in self.bid_book:
            if o.id==id:
                return o
        raise NoOrderWithThisIDInOrderBook

    def amend_quantity(self, id, quantity):
        # Implement this function
        # Hint: Remember that there are two order books, one on the bid side and one on the ask side
        if quantity<=0:
            raise NonPositiveQuantity("New quantity cannot be zero or negative. For zero use Cancel order.")

        order = self.find_order(id)

        if order.quantity<=quantity:
        # You need to raise the following error if the user attempts to modify an order
        # with a quantity that's greater than given in the existing order
            raise NewQuantityNotSmaller("Amendment Must Reduce Quantity!")
        else:
            order.quantity=quantity

    def cancel_order(self, id):
        # Implement this function
        # Think about the changes you need to make in the order book based on the parameters given
        o = self.find_order(id)
        if o.side == OrderSide.BUY:
            self.bid_book.remove(o)
        else:
            self.ask_book.remove(o)


class ActionType(Enum):
    PLACE=1
    AMEND=2
    CANCEL=3
    BALANCE=4
class Exchange():
    def __init__(self):
        super().__init__()
        self.balance = [1000000 for _ in range(100)]
        self.position = [0 for _ in range(100)]# in my understangind these are numbers of position owned by the traders.
        self.matching_engine = MatchingEngine()

    def place_new_order(self, order):
        try:
            self.matching_engine.find_order(order.id)
            return [(order.id,(ActionType.PLACE,order,True))]
        except NoOrderWithThisIDInOrderBook:
            pass
        results = []
        filled_orders = self.matching_engine.handle_order(order)
        for o in filled_orders:
            results.append((o.id,(ActionType.PLACE,o,False)))
            pos_delta = o.quantity if o.side==OrderSide.BUY else -o.quantity
            bal_delta = -o.price*pos_delta
            self.balance[o.id]+=bal_delta
            self.position[o.id]+=pos_delta
        if order.quantity:
            results.append((order.id,(ActionType.PLACE,order,False)))
        return results

    def amend_quantity(self, order_id, quantity):
        amended_successfully = False
        try:
            self.matching_engine.amend_quantity(order_id,quantity)
            amended_successfully = True
        except (NoOrderWithThisIDInOrderBook,NonPositiveQuantity,NewQuantityNotSmaller) as e:
            amended_successfully = False
        return (ActionType.AMEND,amended_successfully,quantity)#I added quantity to response to be able to adjust book_position on the trader's side

    def cancel_order(self, order_id):
        canceled_successfully = False
        try:
            self.matching_engine.cancel_order(order_id)
            canceled_successfully = True
        except NoOrderWithThisIDInOrderBook:
            canceled_successfully = False
        return (ActionType.CANCEL,canceled_successfully)

    def balance_and_position(self, trader_id):
        book_position = 0
        o = None
        try:
            o = self.matching_engine.find_order(trader_id)
            book_position = o.quantity
        except NoOrderWithThisIDInOrderBook:
            book_position = 0
        return (ActionType.BALANCE,(self.balance[trader_id],self.position[trader_id],book_position))

    def convert_dic_from_trader_to_tuple_request(self,dic):
        actionType  = ActionType(dic["ActionType"])
        trader_id = dic["TraderID"]
        if actionType==ActionType.PLACE:
            orderType = OrderType(dic["OrderType"])
            if orderType==OrderType.LIMIT:
                o = LimitOrder(trader_id,dic["Symbol"],dic["Quantity"],dic["Price"],OrderSide(dic["Side"]),dic["Time"])
            elif orderType==OrderType.MARKET:
                o = MarketOrder(trader_id,dic["Symbol"],dic["Quantity"],OrderSide(dic["Side"]),dic["Time"])
            elif orderType==OrderType.IOC:
                o = IOCOrder(trader_id, dic["Symbol"],dic["Price"], dic["Quantity"], OrderSide(dic["Side"]), dic["Time"])
            return (actionType,trader_id,o)
        elif actionType==ActionType.AMEND:
            return (actionType,trader_id,dic["Quantity"])
        elif actionType==ActionType.CANCEL:
            return (actionType,trader_id)
        elif actionType==ActionType.BALANCE:
            return (actionType,trader_id)
        else:
            raise UndefinedTraderAction

    def convert_tuple_from_exchange_to_dic(self,t):
        dic = {"ActionType": t[0].value}
        if t[0] == ActionType.PLACE:
            o = t[1]
            dic["TraderID"] = o.id
            dic["Symbol"] = o.symbol
            dic["Quantity"] = o.quantity
            dic["Side"] = o.side.value
            dic["Time"] = o.time
            if not isinstance(o, MarketOrder):
                dic["Price"] = o.price
            dic["IsFilledOrder"] = int(isinstance(o,FilledOrder))
            dic["OrderPresent"] = int(t[2])
            if not dic["IsFilledOrder"]:
                dic["OrderType"] = o.type.value
            else:
                dic["IsLimit"] = int(o.limit)
        elif t[0] == ActionType.AMEND:
            dic["Successfully"] = int(t[1])
            dic["Quantity"] = t[2]
        elif t[0] == ActionType.CANCEL:
            dic["Successfully"] = int(t[1])
        elif t[0] == ActionType.BALANCE:
            dic["Balance"] = t[1][0]
            dic["Position"] = t[1][1]
            dic["BookPosition"] = t[1][2]
        return dic

    def send_to_trader(self,trader_id,r):
        print_lock.acquire()
        print("Sending the following message to trader with id = {}".format(trader_id))
        print(r)
        print_lock.release()
        send_fixed_len(trader_sockets[trader_id], r)

    def handle_trader_request(self, request):
        actionType = request[0]
        if actionType not in ActionType:
            raise UndefinedTraderAction("Undefined Trader Action!")
        elif actionType==ActionType.PLACE:
            order = request[2]
            results =  self.place_new_order(order)
            for res in results:
                trader_id = res[0]
                response = res[1]
                r = json.dumps(self.convert_tuple_from_exchange_to_dic(response))
                self.send_to_trader(trader_id,r)
        elif actionType==ActionType.AMEND:
            order_id = request[1]
            quantity = request[2]
            r  =json.dumps(self.convert_tuple_from_exchange_to_dic(self.amend_quantity(order_id,quantity)))
            self.send_to_trader(order_id,r)

        elif actionType==ActionType.CANCEL:
            order_id = request[1]
            r = json.dumps(self.convert_tuple_from_exchange_to_dic(self.cancel_order(order_id)))
            self.send_to_trader(order_id,r)

        else: # actionType==ActionType.BALANCE:
            trader_id = request[1]
            r = json.dumps(self.convert_tuple_from_exchange_to_dic(self.balance_and_position(trader_id)))
            self.send_to_trader(trader_id,r)

# I always send messages 1024 bytes long
msg_len = 1024
def my_send(data):
    return bytes(data+" ".join([""]*(msg_len-len(data)+1)),"utf-8")
def send_fixed_len(sock,msg):
    return sock.sendall(my_send(msg))
def recv_fixed_len(sock):
    return sock.recv(msg_len).strip().decode("utf-8")



exchange  = Exchange()


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):

    def handle(self):
        trader_sockets_lock.acquire()
        tr_id = int(recv_fixed_len(self.request))
        trader_sockets[tr_id] = self.request
        trader_sockets_lock.release()
        print_lock.acquire()
        print("Got connection from {}  id = {}".format(self.client_address[0], tr_id))
        print_lock.release()
        send_fixed_len(self.request, str(tr_id))
        time.sleep(5)
        while True:
            # self.request is the TCP socket connected to the client
            rec_data = recv_fixed_len(self.request)
            print_lock.acquire()
            print("Trader with id = {} and ip adress {} wrote:".format(tr_id,self.client_address[0]))
            print(rec_data)
            print_lock.release()
            exchange_lock.acquire()
            t = exchange.convert_dic_from_trader_to_tuple_request(json.loads(rec_data))
            exchange.handle_trader_request(t)
            exchange_lock.release()

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    request_queue_size = 100
    allow_reuse_address = True
    pass

if __name__=="__main__":
    HOST,PORT = "localhost",9999

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    with server:
        ip, port = server.server_address
        try:
            # Start a thread with the server -- that thread will then start one
            # more thread for each request
            server_thread = threading.Thread(target=server.serve_forever)
            # Exit the server thread when the main thread terminates
            server_thread.daemon = True
            server_thread.start()
            print_lock.acquire()
            print("Server loop running in thread:", server_thread.name)
            print_lock.release()
            time.sleep(1)
            while True:
                exchange_lock.acquire()
                bal_sum =  sum(exchange.balance)
                print_lock.acquire()
                print("Sum of balances of all traders at the moment = {}".format(bal_sum))
                print("Their balances are:")
                print("("+", ".join([str(b) for b in exchange.balance])+")")
                time.sleep(2)
                print_lock.release()
                exchange_lock.release()
                time.sleep(5)

        except KeyboardInterrupt:
            print("KeyboardInterrupt.")
            server.shutdown()


