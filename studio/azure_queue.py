import time
import base64

try:
    from azure.storage.queue import QueueService 
except BaseException:
    QueueService = None

from .model import parse_verbosity
from .util import retry
from . import logs


class AzureQueue(object):

    def __init__(self, name, account_name, account_key, verbose=10, receive_timeout=300,
                 retry_time=10):
        assert QueueService is not None
        assert account_name is not None
        assert account_key is not None
        self._client = QueueService(account_name=account_name, 
            account_key=account_key)
        create_q_response = self._client.create_queue(name)

        self._queue_url = "https://{}.queue.core.windows.net/{}".format(
            account_name, name)
        self.logger = logs.getLogger('AzureQueue')
        if verbose is not None:
            self.logger.setLevel(parse_verbosity(verbose))
        self._name = name
        self.logger.info('Creating Azure queue with name ' + name)
        self.logger.info('Queue url = ' + self._queue_url)

        self._receive_timeout = receive_timeout
        self._retry_time = retry_time

    def get_name(self):
        return self._name

    def clean(self, timeout=0):
        while True:
            msg = self.dequeue(timeout=timeout)
            if not msg:
                break

    def enqueue(self, msg):
        self.logger.debug("Sending message {} to queue with url {} "
                          .format(msg, self._queue_url))
        self._client.put_message(self._name, msg)


    def get_all_messages(self):
        seen_ids = []
        messages = []
        not_all = True
        while not_all:
            message = self._client.get_messages(self._name)
            message = message[0]
            if message.id in seen_ids:
                not_all = False
                break
            messages.append(message)
        return messages


    def has_next(self):
        no_tries = 3
        for _ in range(no_tries):
            messages = self.get_all_messages() 
            if len(m) == 0:
                time.sleep(5)
                continue
            else:
                break

        for m in messages:
            self.logger.debug('Received message {} '.format(m.id))
            self.hold(m.id, m.pop_receipt, 0)

        return any(msgs)

    def dequeue(self, acknowledge=True, timeout=0):
        wait_step = 1
        for waited in range(0, timeout + wait_step, wait_step):
            messages = self._client.get_messages(self._name)
            try:
                message = messages[0]
            except:
                continue
            if any(messages):
                break
            elif waited == timeout:
                return None
            else:
                self.logger.info(
                    ('No messages found, sleeping for {} ' +
                     ' (total sleep time {})').format(wait_step, waited))
                time.sleep(wait_step)

        if not any(messages):
            return None

        retval = messages[0]

        if acknowledge:
            self.acknowledge(retval.receipt_handle, retval.id)
            self.logger.debug("Message {} received and acknowledged"
                              .format(retval.id))

            return base64.b64decode(retval.content)
        else:
            self.logger.debug("Message {} received, ack_id {}"
                              .format(retval.id,
                                      retval.receipt_handle))
            return (base64.b64decode(retval.content), retval.receipt_handle)

    def acknowledge(self, ack_id, message_id):
        retry(lambda: self._client.delete_message(
            self._name,
            message_id,
            ack_id),
            sleep_time=10, logger=self.logger)

    def hold(self, message_id, ack_id, minutes):
        self._client.update_message(
            self._name,
            message_id,
            ack_id,
            minutes * 60)

    def delete(self):
        self._client.delete_queue(self._name)
