import socket
import threading
from os import path
from struct import pack
from struct import unpack


class DataPool(object):
    def __init__(self):
        self.__data_list = list()
        self.__lock = threading.Lock()

    def read_data(self, d_path):
        self.__lock.acquire()
        with open(d_path, 'r') as data_file:
            self.__data_list = data_file.readlines()
        self.__lock.release()

    def get_data(self):
        self.__lock.acquire()
        url_data = self.__data_list[:1]
        self.__data_list = self.__data_list[1:]
        self.__lock.release()
        return url_data

    def num_data(self):
        self.__lock.acquire()
        num_data = len(self.__data_list)
        self.__lock.release()
        return num_data


def is_exit(bin_request):
    return len(bin_request) == 4 and unpack('i', bin_request)[0] == -1


def safe_download(sock_conn):  # the end of file must be 'maiguanhua' encoding in utf-8
    _buffer = ''
    while True:
        try:
            sock_conn.settimeout(10)
            _buffer += sock_conn.recv(1048576)
            if len(_buffer) >= 10:
                try:
                    eodata = _buffer[len(_buffer) - 10: len(_buffer)].decode('utf-8')
                    if eodata == 'maiguanhua':
                        print 'maiguanhua found, exit...\n'
                        break
                except UnicodeDecodeError:
                    print 'maiguanhua not found, continue downloading.. ', len(_buffer)
        except socket.timeout:
            print 'socket time out...'
            return _buffer
        finally:
            sock_conn.settimeout(None)
    return _buffer[:len(_buffer) - 10]  # strip end of picture mark


def data_guard(sock_conn, data_pool):
    bin_pic_path = None
    while True:
        #  print 'allocate %d to %s:%s\n' % (cur_id, addr[0], addr[1])
        req = sock_conn.recv(10240)

        if is_exit(req):  # request exit
            break

        if len(req) == 4 and unpack('i', req)[0] == 100:  # 100: request query url
            _url = data_pool.get_data()
            if len(_url) != 0:
                sock_conn.send(_url[0].encode('utf-8'))
            else:
                sock_conn.send('failure'.encode('utf-8'))

        elif len(req) == 4 and unpack('i', req)[0] == 200:  # 200: check if data_pool empty
            num_urls = data_pool.num_data()
            sock_conn.send(pack('i', num_urls))

        elif len(req) == 4 and unpack('i', req)[0] == 300:  # 300: request to post picture path
            sock_conn.send('ready'.encode('utf-8'))
            bin_pic_path = sock_conn.recv(10240)  # receive picture path
            print 'piture path: ', bin_pic_path.decode('utf-8')
            if is_exit(bin_pic_path):
                break
            sock_conn.send('finish'.encode('utf-8'))

        elif len(req) == 4 and unpack('i', req)[0] == 400:  # 400: request to save picture
            sock_conn.send('ready'.encode('utf-8'))
            pic_data = safe_download(sock_conn)  # receive picture data 100KB is not enough...
            if is_exit(pic_data):
                break
            pic_path = bin_pic_path.decode('utf-8')
            with open(pic_path, 'wb') as pic_file:  # save the picture
                print pic_path, 'writing images! length: ', len(pic_data)
                pic_file.write(pic_data)
            sock_conn.send('finish'.encode('utf-8'))

        else:
            print len(req), req
            print 'not match anything!!'
            break
    print 'close service!\n'
    sock_conn.send(pack('i', -1))
    sock_conn.close()


def data_service(local_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('10.214.147.34', local_port))
    sock.listen(10000)

    _data_pool = DataPool()
    _data_pool.read_data(path.join('..', 'url_0_1.2M', 'url_0_1.2M.txt'))
    while True:
        _sock_conn, _addr = sock.accept()
        print _addr
        t = threading.Thread(target=data_guard, args=(_sock_conn, _data_pool))
        t.start()
