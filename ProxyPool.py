import re
import socket
import requests
import threading

from struct import pack
from struct import unpack
from requests.exceptions import SSLError, ConnectionError


class ProxyPool(object):

    def __init__(self, capacity=1000):
        self.__list_proxies = list()
        self.__lock = threading.Lock()
        self.__capacity = capacity

    def __elephant_proxies(self):
        print 'retrieve %d proxies from elephant' % self.__capacity
        pattern = re.compile(r"\^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}\$")
        html_text = ''
        try:
            html = requests.get('http://tpv.daxiangdaili.com/ip/?tid=559561186298989'
                                '&num=%d&delay=1&category=2&foreign=only&filter=on' % self.__capacity)
            if html.status_code != 200:
                return list()  # return empty list
            html_text = html.text
        except SSLError:
            pass
        except ConnectionError:
            pass
        return pattern.findall(html_text)  # return all proxies retrieved from elephant proxy server

    def _produce(self):
        if len(self.__list_proxies) == 0:
            self.__list_proxies = self.__elephant_proxies()

    def get_proxy(self):  # return None if can not find new proxy
        self.__lock.acquire()
        if len(self.__list_proxies) == 0:
            self._produce()
        rslt_proxy = self.__list_proxies[:1]
        self.__list_proxies = self.__list_proxies[1:]
        self.__lock.release()

        return rslt_proxy


def is_exit(bin_request):
    return len(bin_request) == 4 and unpack('i', bin_request)[0] == -1


def proxy_guard(sock_conn, proxy_pool):
    while True:
        #  print 'allocate %d to %s:%s\n' % (cur_id, addr[0], addr[1])
        req = sock_conn.recv(1024)

        if is_exit(req):  # request exit
            break

        if len(req) == 4 and unpack('i', req)[0] == 0:  # 400: request proxy
            _proxies = proxy_pool.get_proxy()
            if len(_proxies) != 0:
                sock_conn.send(pack('i', 888))  # send 888 to tell the data retrieved successfully
                sock_conn.send(_proxies.encode('utf-8'))
            else:
                sock_conn.send(pack('i', 999))  # send 888 to tell the data retrieval failed

        else:
            print 'not match anything!!'
            sock_conn.send(pack('i', -1))  # send a exit signal '-1' to the client

    sock_conn.send(pack('i', -1))
    sock_conn.close()


def proxy_service(local_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', local_port))
    sock.listen(10000)

    _proxy_pool = ProxyPool()
    while True:
        _sock_conn, _addr = sock.accept()
        t = threading.Thread(target=proxy_guard, args=(_sock_conn, _proxy_pool))
        t.start()
