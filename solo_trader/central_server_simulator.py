"""
中心服务器模拟器，模拟交易所随机发单给客户端
"""

# import pprint
import time
import socket
import pickle
import threading
import socketserver
from solo_trader.data_center_simulator import DataCenter


class Handler(socketserver.BaseRequestHandler):

    def handle(self):
        print('Successful Access')
        print('Records Are Below')
        print('=================')
        data_center = DataCenter(init_price=100.00)
        while True:
            data = self.request.recv(1024)
            if data == b'break':
                print('=================')
                break
            print(pickle.loads(data))
            # print(data_center.get_data())
            self.request.send(b'continue')
            time.sleep(0.1)
            # continue


class Server(object):

    def __init__(self, ip, port):
        self.server = socketserver.TCPServer((ip, port), Handler)

    def run(self):
        self.server.serve_forever()

    def stop(self):
        self.server.shutdown()
        print('Server Closed')


class Client(object):

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.content = list(range(10))

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _ = s.connect((self.ip, self.port))
        while True:
            try:
                content = self.content.pop()
            except IndexError:
                s.send(b'break')
                s.close()
                break
            _ = s.send(pickle.dumps(content))
            response = s.recv(1024)
            # if response == b'continue':
            #     continue


if __name__ == "__main__":
    server_simulator = Server('localhost', 9998)
    client_simulator = Client('localhost', 9998)
    thread1 = threading.Thread(target=server_simulator.run)
    thread2 = threading.Thread(target=client_simulator.run)
    thread1.start()
    thread2.start()
    thread2.join()
    server_simulator.stop()
    thread1.join()
