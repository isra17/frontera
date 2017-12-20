# -*- coding: utf-8 -*-
from __future__ import absolute_import
from time import time, sleep
from struct import pack, unpack
from logging import getLogger

import zmq
import six

from frontera.core.messagebus import BaseMessageBus, BaseSpiderLogStream, BaseStreamConsumer, \
    BaseSpiderFeedStream, BaseScoringLogStream, BaseStreamProducer
from frontera.contrib.backends.partitioners import FingerprintPartitioner, Crc32NamePartitioner
from frontera.contrib.messagebus.zeromq.socket_config import SocketConfig
from frontera.utils.misc import load_object
from six.moves import range


class Consumer(BaseStreamConsumer):
    def __init__(self, context, location, partition_id, identity, seq_warnings=False, hwm=1000):
        self.subscriber = context.zeromq.socket(zmq.SUB)
        self.subscriber.connect(location)
        self.subscriber.set(zmq.RCVHWM, hwm)

        filter = identity + pack('>B', partition_id) if partition_id is not None else identity
        self.subscriber.setsockopt(zmq.SUBSCRIBE, filter)
        self.counters = {}
        self.count_global = partition_id is None
        self.logger = getLogger("distributed_frontera.messagebus.zeromq.Consumer(%s-%s)" % (identity, partition_id))
        self.seq_warnings = seq_warnings

        self.stats = context.stats
        self.stat_key = "consumer-%s" % identity
        self.stats[self.stat_key] = 0

    def get_messages(self, timeout=0.1, count=1):
        started = time()
        sleep_time = timeout / 10.0
        while count:
            try:
                msg = self.subscriber.recv_multipart(copy=True, flags=zmq.NOBLOCK)
            except zmq.Again:
                if time() - started > timeout:
                    break
                sleep(sleep_time)
            else:
                partition_seqno, global_seqno = unpack(">II", msg[2])
                from_partition, = unpack(">B", msg[-1])
                seqno = global_seqno if self.count_global else partition_seqno
                if from_partition not in self.counters:
                    self.counters[from_partition] = seqno
                elif self.counters[from_partition] != seqno:
                    if self.seq_warnings:
                        self.logger.warning("Sequence counter mismatch from %d: expected %d, got %d. Check if system "
                                            "isn't missing messages." % (from_partition, self.counters[from_partition], seqno))
                    self.counters[from_partition] = seqno
                yield msg[1]
                count -= 1
                self.counters[from_partition] += 1
                self.logger.debug('Counter: {}={} ({}, {}) from {}'.format(seqno, self.counters[from_partition], global_seqno, partition_seqno, from_partition))
                self.stats[self.stat_key] += 1

    def get_offset(self, partition_id):
        if self.counters:
            return max(self.counters.values())
        return 0


class Producer(BaseStreamProducer):
    def __init__(self, context, location, identity, partition=None):
        self.spider_partition = partition
        self.identity = identity
        self.sender = context.zeromq.socket(zmq.PUB)
        self.sender.connect(location)
        self.counters = {}
        self.global_counter = 0
        self.stats = context.stats
        self.stat_key = "producer-%s" % identity
        self.stats[self.stat_key] = 0

    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")
        partition = self.partition(key)
        counter = self.counters.get(partition, 0)
        for msg in messages:
            self.sender.send_multipart([self.identity + pack(">B", partition), msg,
                                        pack(">II", counter, self.global_counter),
                                        pack(">B", self.spider_partition or 0)])
            counter += 1
            self.global_counter += 1
            if counter == 4294967296:
                counter = 0
            if self.global_counter == 4294967296:
                self.global_counter = 0
            self.stats[self.stat_key] += 1
        self.counters[partition] = counter

    def flush(self):
        pass

    def get_offset(self, partition_id):
        return self.counters.get(partition_id, None)

    def partition(self, key):
        return self.partitioner.partition(key)


class SpiderLogProducer(Producer):
    def __init__(self, context, location, partitioner, partition):
        super(SpiderLogProducer, self).__init__(context, location, b'sl', partition)
        self.partitioner = partitioner


class SpiderLogStream(BaseSpiderLogStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.sw_in_location = messagebus.socket_config.sw_in()
        self.db_in_location = messagebus.socket_config.db_in()
        self.out_location = messagebus.socket_config.spiders_out()
        self.partitioner = messagebus.spider_log_partitioner
        self.partition = messagebus.spider_partition

    def producer(self):
        return SpiderLogProducer(self.context, self.out_location, self.partitioner, self.partition)

    def consumer(self, partition_id, type):
        location = self.sw_in_location if type == b'sw' else self.db_in_location
        return Consumer(self.context, location, partition_id, b'sl', seq_warnings=True)


class UpdateScoreProducer(Producer):
    def __init__(self, context, location):
        super(UpdateScoreProducer, self).__init__(context, location, b'us')

    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")
        counter = self.counters.get(0, 0)
        for msg in messages:
            self.sender.send_multipart([self.identity, msg, pack(">II", counter, counter)])
            counter += 1
            if counter == 4294967296:
                counter = 0
            self.stats[self.stat_key] += 1
        self.counters[0] = counter


class ScoringLogStream(BaseScoringLogStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.in_location = messagebus.socket_config.sw_out()
        self.out_location = messagebus.socket_config.db_in()

    def consumer(self):
        return Consumer(self.context, self.out_location, None, b'us')

    def producer(self):
        return UpdateScoreProducer(self.context, self.in_location)


class SpiderFeedProducer(Producer):
    def __init__(self, context, location, hwm, partitioner):
        super(SpiderFeedProducer, self).__init__(context, location, b'sf')
        self.partitioner = partitioner
        self.sender.set(zmq.SNDHWM, hwm)


class SpiderFeedStream(BaseSpiderFeedStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.in_location = messagebus.socket_config.db_out()
        self.out_location = messagebus.socket_config.spiders_in()
        self.partitioner = messagebus.spider_feed_partitioner
        self.partitions_offset = {}
        for partition_id in self.partitioner.partitions:
            self.partitions_offset[partition_id] = 0
        self.consumer_hwm = messagebus.spider_feed_rcvhwm
        self.producer_hwm = messagebus.spider_feed_sndhwm
        self.max_next_requests = messagebus.max_next_requests
        self._producer = None

    def consumer(self, partition_id):
        return Consumer(self.context, self.out_location, partition_id, b'sf', seq_warnings=True, hwm=self.consumer_hwm)

    def producer(self):
        if not self._producer:
            self._producer = SpiderFeedProducer(self.context, self.in_location,
                                                self.producer_hwm, self.partitioner)
        return self._producer

    def available_partitions(self):
        if not self._producer:
            return []

        partitions = []
        for partition_id, last_offset in self.partitions_offset.items():
            producer_offset = self._producer.get_offset(partition_id)
            if producer_offset is None:
                producer_offset = 0
            lag = producer_offset - last_offset
            if lag < self.max_next_requests or not last_offset:
                partitions.append(partition_id)
        return partitions

    def set_spider_offset(self, partition_id, offset):
        self.partitions_offset[partition_id] = offset


class Context(object):

    zeromq = zmq.Context()
    stats = {}


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        self.logger = getLogger("messagebus.zeromq")
        self.context = Context()
        self.socket_config = SocketConfig(settings.get('ZMQ_ADDRESS'),
                                          settings.get('ZMQ_BASE_PORT'))
        self.spider_partition = settings.get('SPIDER_PARTITION_ID')

        if settings.get('QUEUE_HOSTNAME_PARTITIONING'):
            self.logger.warning('QUEUE_HOSTNAME_PARTITIONING is deprecated, use SPIDER_FEED_PARTITIONER instead.')
            settings.set('SPIDER_FEED_PARTITIONER', 'frontera.contrib.backends.partitioners.Crc32NamePartitioner')

        self.spider_log_partitions = [i for i in range(settings.get('SPIDER_LOG_PARTITIONS'))]
        spider_log_partitioner_cls = load_object(settings.get('SPIDER_LOG_PARTITIONER'))
        self.spider_log_partitioner = spider_log_partitioner_cls(self.spider_log_partitions)

        self.spider_feed_partitions = [i for i in range(settings.get('SPIDER_FEED_PARTITIONS'))]
        spider_feed_partitioner_cls = load_object(settings.get('SPIDER_FEED_PARTITIONER'))
        self.spider_feed_partitioner = spider_feed_partitioner_cls(self.spider_feed_partitions)

        self.spider_feed_sndhwm = int(settings.get('MAX_NEXT_REQUESTS') * len(self.spider_feed_partitions) * 1.2)
        self.spider_feed_rcvhwm = int(settings.get('MAX_NEXT_REQUESTS') * 2.0)
        self.max_next_requests = int(settings.get('MAX_NEXT_REQUESTS'))
        if self.socket_config.is_ipv6:
            self.context.zeromq.setsockopt(zmq.IPV6, True)

    def spider_log(self):
        return SpiderLogStream(self)

    def scoring_log(self):
        return ScoringLogStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)

