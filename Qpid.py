from qpid.messaging import Connection
from qpid.messaging import MessagingError

class QpidConnection():

    port = None
    ip = None
    data_queue = None
    cmd_queue = None
    conn = None
    session = None
    sender = None
    receiver = None

    def __init__(self, ip, port, data_queue, cmd_queue):
            self.ip = ip;
            self.port = port
            self.data_queue = data_queue
            self.cmd_queue = cmd_queue

    def start(self):
        try:
            url = str(self.ip)
            url += str(':')
            url += str(self.port)
            self.conn = Connection(url)
            self.conn.open()
            self.session = self.conn.session()
            self.sender = self.session.sender(self.cmd_queue)
            self.receiver = self.session.receiver(self.data_queue)
            return 1
        except MessagingError as m:
            print(m)
            return 0

    def stop(self):
        try:
            self.conn.close()
        except MessagingError as m:
            print(m)
            return 0