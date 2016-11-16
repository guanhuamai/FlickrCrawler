import threading
from DataPool import data_service
from ProxyPool import proxy_service


if __name__ == '__main__':
    data_server = threading.Thread(target=data_service, args=(9998,))  # open data server at port 9998
    proxy_server = threading.Thread(target=proxy_service, args=(9999,))  # opend proxy server at port 9999
    data_server.start()
    proxy_server.start()
