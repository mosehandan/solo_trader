"""
中心服务器模拟器，模拟交易所随机发单给客户端
"""

# import pprint
from solo_trader.data_center_simulator import DataCenter
import socket
import pickle
import threading
import socketserver


class Handler(socketserver.BaseRequestHandler):

    def handle(self):
        print('Successful Access')
        print('Records Are Below')
        print('=================')
        while True:
            data = self.request.recv(1024)
            if data == b'break':
                print('=================')
                break
            print(pickle.loads(data))
            self.request.send(b'continue')
            continue


class Server(object):

    def __init__(self, ip, port):
        self.server = socketserver.TCPServer((ip, port), Handler)

    def run(self):
        self.server.serve_forever()

    def stop(self):
        self.server.shutdown()
        print('Server Closed')


class Trader(object):

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.orders = self.get_order()

    @staticmethod
    def get_order():
        return [1, 2, 3]

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _ = s.connect((self.ip, self.port))
        while True:
            try:
                content = self.orders.pop()
            except IndexError:
                s.send(b'break')
                s.close()
                break
            _ = s.send(pickle.dumps(content))
            response = s.recv(1024)
            if response == b'continue':
                continue


if __name__ == "__main__":
    server = Server('localhost', 9998)
    trader = Trader('localhost', 9998)
    thread1 = threading.Thread(target=server.run)
    thread2 = threading.Thread(target=trader.run)
    thread1.start()
    thread2.start()
    thread2.join()
    server.stop()
    thread1.join()
