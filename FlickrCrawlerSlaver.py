import re
import socket
from socket import error as sock_error
import requests
import gevent
from gevent import monkey
from struct import pack
from bs4 import BeautifulSoup
from requests.exceptions import SSLError, ConnectionError, Timeout, ChunkedEncodingError
from os import path
import threading

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
            except ChunkedEncodingError:
                _proxy = None
    return _proxy


def get_next_url(sock_conn):  # send, recv
    _url = None
    while _url is None:
        sock_conn.send(pack('i', 100))
        try:
            ret = sock_conn.recv(10240).decode('utf-8')
            if ret == 'failure':
                break
            else:
                _url = ret
        except UnicodeDecodeError:
            break
        except sock_error:
            break
    return _url


def post_path(img_path, sock_conn, d_lock):  # send, recv, send
    d_lock.acquire()
    try:
        sock_conn.send(pack('i', 300))
        ret = sock_conn.recv(1024).decode('utf-8')
        if ret == 'ready':
            sock_conn.send(img_path.encode('utf-8'))
        else:
            sock_conn.send(pack('i', -1))
        print 'upload picture path', sock_conn.recv(1024).decode('utf-8')
    finally:
        d_lock.release()


def save_pic(img_content, sock_conn, d_lock):
    d_lock.acquire()
    try:
        sock_conn.send(pack('i', 400))
        ret = sock_conn.recv(1024).decode('utf-8')
        if ret == 'ready':
            eodata = 'maiguanhua'.encode('utf-8')
            sock_conn.send(img_content + eodata)
        else:
            sock_conn.send(pack('i', -1))
        print 'save picture', sock_conn.recv(1024).decode('utf-8')
    finally:
        d_lock.release()


def write_img(img_content, img_path, sock_conn, d_lock):
    post_path(img_path, sock_conn, d_lock)
    save_pic(img_content, sock_conn, d_lock)


def get_next_task(sock_conn, d_lock):
    str_task = None, None
    d_lock.acquire()
    try:
        id_url = get_next_url(sock_conn)
        if id_url is not None:
            str_id = id_url.split(' ')[0]
            str_url = id_url.split(' ')[1].strip('\n')
            str_task = str_id, str_url
    finally:
        d_lock.release()
    return str_task


def corout_crawl(p_addr, d_addr, corout_id):
    _proxy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _proxy_sock.connect(p_addr)
    _data_sock.connect(d_addr)

    _data_lock = threading.Lock()

    _proxy = get_active_proxy(_proxy_sock)

    pattern1 = re.compile(r"http://www\.flickr\.com/photos/.*")
    pattern2 = re.compile(r"http://.*\.jpg")

    _id, _url = get_next_task(_data_sock, _data_lock)
    cnt = 0
    while True:
        cnt += 1
        print '%d coroute requesting %d %s\n' % (corout_id, cnt, _url)
        if _proxy is None or _id is None or _url is None:
            break
        proxies = {'http': 'http://' + _proxy, 'https': 'https://' + _proxy}

        if pattern1.match(_url):
            try:
                html = requests.get(_url, proxies=proxies, timeout=8)
                bs_obj = BeautifulSoup(html.text, 'html.parser')
                img_url = bs_obj.find("img", {"src": re.compile("//.*\.staticflickr\.com/.*\.jpg")})['src']

                resp = requests.get('http:' + img_url, proxies=proxies, timeout=8)
                if resp.status_code == 200:
                    write_img(resp.content, path.join('..', 'FlickrPictures', _id+'.jpg'), _data_sock, _data_lock)
                    _id, _url = get_next_task(_data_sock, _data_lock)
            except SSLError:
                _proxy = get_active_proxy(_proxy_sock)
            except ConnectionError:
                _proxy = get_active_proxy(_proxy_sock)
            except Timeout:
                _proxy = get_active_proxy(_proxy_sock)
            except ChunkedEncodingError:
                _proxy = get_active_proxy(_proxy_sock)
            except TypeError:
                _id, _url = get_next_task(_data_sock, _data_lock)

        elif pattern2.match(_url):
            try:
                resp = requests.get(_url, proxies=proxies, timeout=8)
                if resp.status_code == 200:
                    write_img(resp.content, path.join('..', 'FlickrPictures2', _id + '.jpg'), _data_sock, _data_lock)
                    _id, _url = get_next_task(_data_sock, _data_lock)
            except SSLError:
                _proxy = get_active_proxy(_proxy_sock)
            except ConnectionError:
                _proxy = get_active_proxy(_proxy_sock)
            except Timeout:
                _proxy = get_active_proxy(_proxy_sock)
            except ChunkedEncodingError:
                _proxy = get_active_proxy(_proxy_sock)
        else:
            print 'unknown format %s\n' % _url
            raise Exception

    _proxy_sock.close()
    _data_sock.close()


def slave_do(p_addr, d_addr):  # addr = ('127.0.0.1', 9999)
    gevent.joinall([gevent.spawn(corout_crawl, p_addr, d_addr, i) for i in range(200)])

if __name__ == '__main__':
    slave_do(('10.214.147.34', 9999), ('10.214.147.34', 9998))
