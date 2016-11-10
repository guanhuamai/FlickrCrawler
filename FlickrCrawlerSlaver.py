import re
import socket
import requests
import gevent
from gevent import monkey
from struct import pack
from struct import unpack
from bs4 import BeautifulSoup
from requests.exceptions import SSLError, ConnectionError

monkey.patch_all()


def get_active_proxy(sock_conn):
    _proxy = None
    while _proxy is None:
        sock_conn.send(pack('i', 400))
        status_code = unpack('i', sock_conn.recv(1024))
        if status_code == 888:
            _proxy = sock_conn.recv(1024).decode('utf-8')
            try:
                html = requests.get('https://www.baidu.com',
                                    proxies={'http': 'http://'+_proxy, 'https': 'https://'+_proxy})
                if html.status_code != 200:
                    _proxy = None
            except SSLError:
                _proxy = None
            except ConnectionError:
                _proxy = None

        elif status_code == 999:
            print 'get proxy failed'
            break

    return _proxy


def get_next_url(sock_conn):
    _url = None
    while _url is None:
        sock_conn.send(pack('i', 100))
        status_code = unpack('i', sock_conn.recv(1024))
        if status_code == 888:
            _url = sock_conn.recv(1024).decode('utf-8')

        elif status_code == 999:
            print 'get url failed, no more url perhaps'
            break
    return _url


def write_img(img_content, img_path, sock_conn):
    sock_conn.send(pack('i', 300))
    sock_conn.send(img_path.encode('utf-8'))
    sock_conn.send(img_content)


def corout_crawl(p_addr, d_addr):
    _proxy_sock= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _proxy_sock.connect(p_addr)
    _data_sock.connect(d_addr)

    while True:
        _proxy = get_active_proxy(_proxy_sock)
        if _proxy is None:
            break
        _url = get_next_url(_data_sock)
        if _url is None:
            break

        proxies = {'http': 'http://'+_proxy, 'https': 'https://'+_proxy}
        print _url
        try:
            html = requests.get(_url, proxies=proxies)

            bs_obj = BeautifulSoup(html.text)
            img_url = bs_obj.find("img", {"src": re.compile("//.*\.staticflickr\.com/.*\.jpg")})['src']

            resp = requests.get('http:' + img_url, proxies=proxies)
            if resp.status_code == 200:
                write_img(resp, 'not written yet..', _data_sock)
        except SSLError:
            pass
        except ConnectionError:
            pass

    _proxy_sock.send(pack('i', -1))
    _data_sock.send(pack('i', -1))
    _proxy_sock.close()
    _data_sock.close()


def slave_do(p_addr, d_addr):  # addr = ('127.0.0.1', 9999)
    for _ in range(1000):
        gevent.joinall([gevent.spawn(corout_crawl, (p_addr, d_addr))])

