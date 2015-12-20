from functools import partial
from datetime import timedelta

from tornado import gen
from tornadoredis import Client
from tornadoredis import ConnectionPool
from tornadoredis.exceptions import ResponseError
from tornadoredis.pubsub import BaseSubscriber


class CelerySubscriber(BaseSubscriber):
    def unsubscribe_channel(self, channel_name):
        """Unsubscribes the redis client from the channel"""
        del self.subscribers[channel_name]
        del self.subscriber_count[channel_name]
        self.redis.unsubscribe(channel_name)

    def on_message(self, msg):
        if not msg:
            return
        if msg.kind == 'message' and msg.body:
            # Get the list of subscribers for this channel
            for subscriber in self.subscribers[msg.channel].keys():
                subscriber(msg.body)
        super(CelerySubscriber, self).on_message(msg)


class RedisClient(Client):
    @gen.engine
    def _consume_bulk(self, tail, callback=None):
        response = yield gen.Task(self.connection.read, int(tail) + 2)
        if isinstance(response, Exception):
            raise response
        if not response:
            raise ResponseError('EmptyResponse')
        else:
            # We don't cast try to convert to unicode here as the response
            # may not be utf-8 encoded, for example if using msgpack as a
            # serializer
            # response = to_unicode(response)
            response = response[:-2]
        callback(response)


class RedisConsumer(object):
    def __init__(self, producer):
        self.producer = producer
        self.client = self.create_redis_client()
        self.subscriber = CelerySubscriber(self.create_redis_client())

    def wait_for(self, task_id, callback, expires=None, persistent=None):
        key = self.backend.get_key_for_task(task_id)
        current_subscribed = [False]

        def _set_subscribed(subscribed):
            current_subscribed[0] = subscribed

        def _on_timeout():
            _set_subscribed(True)
            self.on_timeout(key)

        # Expiry time of the task should be used rather than the result ? ? ?
        if expires:
            timeout = self.io_loop.add_timeout(timedelta(milliseconds=expires), _on_timeout)
        else:
            timeout = None
        current_subscriber = partial(self.on_result, key, callback, timeout)
        self.io_loop.add_future(gen.Task(self.subscriber.subscribe, key, current_subscriber),
                                lambda future: _set_subscribed(future.result()))

        # If the task is completed before subscription,
        # we would not get the result.So we try to check this case
        # until the subscription is completed.
        def _check_subscribed(future):
            result = future.result()
            if result:
                current_subscriber(result)
            elif not current_subscribed[0]:
                self.io_loop.add_future(gen.Task(self.client.get, key), _check_subscribed)
        self.io_loop.add_future(gen.Task(self.client.get, key), _check_subscribed)

    def on_result(self, key, callback, timeout, result):
        if timeout:
            self.producer.conn_pool.io_loop.remove_timeout(timeout)
        self.subscriber.unsubscribe_channel(key)
        callback(result)

    def on_timeout(self, key):
        self.subscriber.unsubscribe_channel(key)

    def create_redis_client(self):
        return RedisClient(password=self.backend.connparams['password'],
                           selected_db=self.backend.connparams['db'],
                           connection_pool=self.connection_pool,
                           io_loop=self.io_loop)

    @property
    def connection_pool(self):
        if not hasattr(RedisConsumer, '_connection_pool'):
            self._connection_pool = ConnectionPool(
                host=self.backend.connparams['host'],
                port=self.backend.connparams['port'],
                io_loop=self.io_loop)
        return self._connection_pool

    @property
    def io_loop(self):
        return self.producer.conn_pool.io_loop

    @property
    def backend(self):
        return self.producer.app.backend
