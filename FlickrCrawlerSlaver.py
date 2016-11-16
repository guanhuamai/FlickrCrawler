import re
import socket
import requests
import gevent
from gevent import monkey
from struct import pack
from bs4 import BeautifulSoup
from requests.exceptions import SSLError, ConnectionError, Timeout
from os import path

monkey.patch_all()


def get_active_proxy(sock_conn):
    _proxy = None
    while _proxy is None:
        sock_conn.send(pack('i', 400))
        ret = sock_conn.recv(10240).decode('utf-8')
        if ret != 'failure':
            _proxy = ret
            try:
                proxies = {'http': 'http://'+_proxy, 'https': 'https://'+_proxy}
                html = requests.get('https://www.baidu.com', proxies=proxies, timeout=8)
                if html.status_code != 200:
                    _proxy = None
            except SSLError:
                _proxy = None
            except ConnectionError:
                _proxy = None
            except Timeout:
                _proxy = None
    print 'successfully got proxy %s\n' % _proxy
    return _proxy


def get_next_url(sock_conn):
    _url = None
    while _url is None:
        sock_conn.send(pack('i', 100))
        ret = sock_conn.recv(10240).decode('utf-8')
        if ret == 'failure':
            print 'get url failed, no more url perhaps'
            break
        else:
            _url = ret
    return _url


def write_img(img_content, img_path, sock_conn):
    sock_conn.send(pack('i', 300))
    ret = sock_conn.recv(1024).decode('utf-8')
    if ret == 'ready':
        sock_conn.send(img_path.encode('utf-8'))
        if sock_conn.recv(1024).decode('utf-8') == 'ready':
            sock_conn.send(img_content)
        else:
            sock_conn.send(pack('i', -1))
    else:
        sock_conn.send(pack('i', -1))


def get_next_task(sock_conn):
    id_url = get_next_url(sock_conn)
    if id_url is None:
        return None, None
    str_id = id_url.split(' ')[0]
    str_url = id_url.split(' ')[1].strip('\n')
    return str_id, str_url


def corout_crawl(p_addr, d_addr):
    _proxy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _proxy_sock.connect(p_addr)
    _data_sock.connect(d_addr)

    _proxy = get_active_proxy(_proxy_sock)

    pattern1 = re.compile(r"http://www\.flickr\.com/photos/.*")
    pattern2 = re.compile(r"http://.*\.jpg\$")

    _id, _url = get_next_task(_data_sock)
    while True:
        if _proxy is None or _id is None or _url is None:
            break
        proxies = {'http': 'http://' + _proxy, 'https': 'https://' + _proxy}
        print _id, _url

        if pattern1.match(_url):
            try:
                html = requests.get(_url, proxies=proxies, timeout=8)
                bs_obj = BeautifulSoup(html.text, 'html.parser')
                img_url = bs_obj.find("img", {"src": re.compile("//.*\.staticflickr\.com/.*\.jpg")})['src']

                resp = requests.get('http:' + img_url, proxies=proxies, timeout=8)
                if resp.status_code == 200:
                    print 'start writing image %s' % _id+'.jpg'
                    write_img(resp.content, path.join('..', 'FlickrPictures', _id+'.jpg'), _data_sock)
                    _id, _url = get_next_task(_data_sock)
            except SSLError:
                print 'ssl error, retrieve another proxy...\n'
                _proxy = get_active_proxy(_proxy_sock)
            except ConnectionError:
                print 'connection error, retrieve another proxy...\n'
                _proxy = get_active_proxy(_proxy_sock)
            except Timeout:
                print 'timeout error, retrieve another proxy...\n'
                _proxy = get_active_proxy(_proxy_sock)
            except TypeError:
                print 'type error...'
                _id, _url = get_next_task(_data_sock)

        elif pattern2.match(_url):
            try:
                resp = requests.get(_url, proxies=proxies, timeout=8)
                if resp.status_code == 200:
                    write_img(resp.content, path.join('FlickrPictures', _id+'.jpg'), _data_sock)
            except SSLError:
                print 'ssl error, retrieve another proxy...\n'
                _proxy = get_active_proxy(_proxy_sock)
            except ConnectionError:
                print 'connection error, retrieve another proxy...\n'
                _proxy = get_active_proxy(_proxy_sock)
            except Timeout:
                print 'timeout error, retrieve another proxy...\n'
                _proxy = get_active_proxy(_proxy_sock)

    _proxy_sock.send(pack('i', -1))
    _data_sock.send(pack('i', -1))
    _proxy_sock.close()
    _data_sock.close()


def slave_do(p_addr, d_addr):  # addr = ('127.0.0.1', 9999)
    gevent.joinall([gevent.spawn(corout_crawl, p_addr, d_addr) for _ in range(100)])

if __name__ == '__main__':
    slave_do(('127.0.0.1', 9999), ('127.0.0.1', 9998))
