# -*- coding: utf-8 -*-
from __future__ import absolute_import
import signal
import logging
from traceback import format_stack
from logging.config import fileConfig
from argparse import ArgumentParser
from time import asctime
from os.path import exists
from collections import defaultdict

from twisted.internet import reactor, task
from twisted.python.failure import Failure
from frontera.core.components import DistributedBackend
from frontera.core.manager import FrontierManager
from frontera.logger.handlers import CONSOLE

from frontera.settings import Settings
from frontera.utils.misc import load_object
from frontera.utils.async import CallLaterOnce
from .server import WorkerJsonRpcService
import six
from six.moves import map

logger = logging.getLogger("db-worker")


class Slot(object):
    def __init__(self, new_batch, consume_incoming, consume_scoring, no_batches, no_scoring_log,
                 new_batch_delay, no_spider_log):
        self.new_batch = CallLaterOnce(new_batch)
        self.new_batch.setErrback(self.error)

        self.consumption = CallLaterOnce(consume_incoming)
        self.consumption.setErrback(self.error)

        self.scheduling = CallLaterOnce(self.schedule)
        self.scheduling.setErrback(self.error)

        self.scoring_consumption = CallLaterOnce(consume_scoring)
        self.scoring_consumption.setErrback(self.error)

        self.no_batches = no_batches
        self.no_scoring_log = no_scoring_log
        self.no_spider_log = no_spider_log
        self.new_batch_delay = new_batch_delay

    def error(self, f):
        if isinstance(f, Failure):
            logger.error(f.value, exc_info=(f.type, f.value, f.getTracebackObject()))
        else:
            self.exception(f.value)
        return f

    def schedule(self, on_start=False):
        if on_start and not self.no_batches:
            self.new_batch.schedule(0)

        if not self.no_spider_log:
            self.consumption.schedule()
        if not self.no_batches:
            self.new_batch.schedule(self.new_batch_delay)
        if not self.no_scoring_log:
            self.scoring_consumption.schedule()
        self.scheduling.schedule(5.0)


class DBWorker(object):
    def __init__(self, settings, no_batches, no_incoming, no_scoring):
        messagebus = load_object(settings.get('MESSAGE_BUS'))
        self.mb = messagebus(settings)
        spider_log = self.mb.spider_log()

        self.spider_feed = self.mb.spider_feed()
        self.spider_log_consumer = spider_log.consumer(partition_id=None, type=b'db')
        self.spider_feed_producer = self.spider_feed.producer()

        self._manager = FrontierManager.from_settings(settings, db_worker=True)
        self._backend = self._manager.backend
        codec_path = settings.get('MESSAGE_BUS_CODEC')
        encoder_cls = load_object(codec_path+".Encoder")
        decoder_cls = load_object(codec_path+".Decoder")
        self._encoder = encoder_cls(self._manager.request_model)
        self._decoder = decoder_cls(self._manager.request_model, self._manager.response_model)

        if isinstance(self._backend, DistributedBackend) and not no_scoring:
            scoring_log = self.mb.scoring_log()
            self.scoring_log_consumer = scoring_log.consumer()
            self.queue = self._backend.queue
            self.strategy_disabled = False
        else:
            self.strategy_disabled = True
        self.spider_log_consumer_batch_size = settings.get('SPIDER_LOG_CONSUMER_BATCH_SIZE')
        self.scoring_log_consumer_batch_size = settings.get('SCORING_LOG_CONSUMER_BATCH_SIZE')

        if settings.get('QUEUE_HOSTNAME_PARTITIONING'):
            self.logger.warning('QUEUE_HOSTNAME_PARTITIONING is deprecated, use SPIDER_FEED_PARTITIONER instead.')
            settings.set('SPIDER_FEED_PARTITIONER', 'frontera.contrib.backends.partitioners.Crc32NamePartitioner')
        self.partitioner_cls = load_object(settings.get('SPIDER_FEED_PARTITIONER'))
        self.max_next_requests = settings.MAX_NEXT_REQUESTS
        self.slot = Slot(self.new_batch, self.consume_incoming, self.consume_scoring, no_batches,
                         self.strategy_disabled, settings.get('NEW_BATCH_DELAY'), no_incoming)
        self.job_id = 0
        self.stats = {
            'consumed_since_start': 0,
            'consumed_scoring_since_start': 0,
            'pushed_since_start': 0
        }
        self._logging_task = task.LoopingCall(self.log_status)

    def remote_debug_signal(self, *args):
        from remote_pdb import RemotePdb
        RemotePdb('0.0.0.0', 9999).set_trace()

    def set_process_info(self, process_info):
        self.process_info = process_info

    def run(self):
        self.slot.schedule(on_start=True)
        self._logging_task.start(30)
        signal.signal(signal.SIGUSR1, self.remote_debug_signal)
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        reactor.run()

    def stop(self):
        logger.info("Stopping frontier manager.")
        self._manager.stop()

    def log_status(self):
        for k, v in six.iteritems(self.stats):
            logger.info("%s=%s", k, v)

        spider_offsets = self.spider_feed.partitions_offset
        worker_offsets = self.spider_feed.producer().counters
        overused = {p:len(o) for p, o in self._backend.overused.items()}
        logger.info('spider_offsets={!r}'.format(spider_offsets))
        logger.info('worker_offsets={!r}'.format(worker_offsets))
        logger.info('overused={!r}'.format(overused))

    def disable_new_batches(self):
        self.slot.no_batches = True

    def enable_new_batches(self):
        self.slot.no_batches = False

    def consume_incoming(self, *args, **kwargs):
        consumed = 0
        for m in self.spider_log_consumer.get_messages(timeout=1.0, count=self.spider_log_consumer_batch_size):
            try:
                msg = self._decoder.decode(m)
            except (KeyError, TypeError) as e:
                logger.error("Decoding error: %s", e)
                continue
            else:
                try:
                    type = msg[0]
                    if type == 'add_seeds':
                        _, seeds = msg
                        logger.info('Adding %i seeds', len(seeds))
                        for seed in seeds:
                            logger.debug('URL: %s', seed.url)
                        self._backend.add_seeds(seeds)
                        continue
                    if type == 'page_crawled':
                        _, response = msg
                        request = response.request
                        request_fingerprint = request.meta.get(b'fingerprint').decode() if request else None

                        logger.debug("Page crawled %s [%s from %s]", response.url, response.meta.get(b'fingerprint').decode(), request_fingerprint)
                        if b'jid' not in response.meta or response.meta[b'jid'] != self.job_id:
                            self.logger.warning('Response {} has no jid'.format(response))
                            continue
                        self._backend.page_crawled(response)
                        continue
                    if type == 'links_extracted':
                        _, request, links = msg
                        logger.debug("Links extracted %s (%d) [%s]", request.url, len(links), request.meta.get(b'fingerprint').decode())
                        if b'jid' not in request.meta or request.meta[b'jid'] != self.job_id:
                            self.logger.warning('Response {} has no jid'.format(request))
                            continue
                        self._backend.links_extracted(request, links)
                        continue
                    if type == 'request_error':
                        _, request, error = msg
                        logger.debug("Request error %s [%s]", request.url, request.meta.get(b'fingerprint'))
                        if b'jid' not in request.meta or request.meta[b'jid'] != self.job_id:
                            self.logger.warning('Response {} has no jid'.format(request))
                            continue
                        self._backend.request_error(request, error)
                        continue
                    if type == 'offset':
                        _, partition_id, offset = msg
                        logger.debug('Offset %s=%s', partition_id, offset)
                        self.spider_feed.set_spider_offset(partition_id, offset)
                        continue
                    if type == 'overused':
                        if not hasattr(self._backend, 'set_overused'):
                            continue
                        _, partition_id, netlocs = msg
                        for netloc in netlocs:
                            logger.debug('Domain: %s', netloc)
                        self._backend.set_overused(partition_id, netlocs)
                        continue
                    logger.debug('Unknown message type %s', type)
                except Exception as exc:
                    logger.exception(exc)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug("Message caused the error %s", str(msg))
                    continue
            finally:
                consumed += 1
        """
        # TODO: Think how it should be implemented in DB-worker only mode.
        if not self.strategy_disabled and self._backend.finished():
            logger.info("Crawling is finished.")
            reactor.stop()
        """
        self.stats['consumed_since_start'] += consumed
        self.stats['last_consumed'] = consumed
        self.stats['last_consumption_run'] = asctime()
        self.slot.schedule()
        return consumed

    def consume_scoring(self, *args, **kwargs):
        consumed = 0
        seen = set()
        batch = []
        for m in self.scoring_log_consumer.get_messages(count=self.scoring_log_consumer_batch_size):
            try:
                msg = self._decoder.decode(m)
            except (KeyError, TypeError) as e:
                logger.error("Decoding error: %s", e)
                continue
            else:
                if msg[0] == 'update_score':
                    _, request, score, schedule = msg
                    if request.meta[b'fingerprint'] not in seen:
                        batch.append((request.meta[b'fingerprint'], score, request, schedule))
                        seen.add(request.meta[b'fingerprint'])
                if msg[0] == 'new_job_id':
                    self.job_id = msg[1]
            finally:
                consumed += 1
        self.queue.schedule(batch)

        self.stats['consumed_scoring_since_start'] += consumed
        self.stats['last_consumed_scoring'] = consumed
        self.stats['last_consumption_run_scoring'] = asctime()
        self.slot.schedule()

    def new_batch(self, *args, **kwargs):
        partitions = self.spider_feed.available_partitions()
        self._backend.on_new_batch(partitions)
        logger.info("Getting new batches for partitions %s" % str(",").join(map(str, partitions)))
        if not partitions:
            return 0

        count = 0
        partitions_count = defaultdict(lambda: 0)

        for request in self._backend.get_next_requests(self.max_next_requests, partitions=partitions):
            try:
                request.meta[b'jid'] = self.job_id
                eo = self._encoder.encode_request(request)
            except Exception as e:
                logger.error("Encoding error, %s, fingerprint: %s, url: %s" % (e,
                                                                               request.meta[b'fingerprint'],
                                                                               request.url))
                continue
            finally:
                count += 1
            key = self.spider_feed_producer.partitioner.get_key(request)
            self.spider_feed_producer.send(key, eo)
            partition_id = self.spider_feed_producer.partition(key)
            partitions_count[partition_id] += 1

        logger.info('Sent batches: {!r}'.format(dict(partitions_count)))
        self.stats['pushed_since_start'] += count
        self.stats['last_batch_size'] = count
        self.stats.setdefault('batches_after_start', 0)
        self.stats['batches_after_start'] += 1
        self.stats['last_batch_generated'] = asctime()
        return count


if __name__ == '__main__':
    parser = ArgumentParser(description="Frontera DB worker.")
    parser.add_argument('--no-batches', action='store_true',
                        help='Disables generation of new batches.')
    parser.add_argument('--no-incoming', action='store_true',
                        help='Disables spider log processing.')
    parser.add_argument('--no-scoring', action='store_true',
                        help='Disables scoring log processing.')
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import.')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL.")
    parser.add_argument('--port', type=int, help="Json Rpc service port to listen.")
    args = parser.parse_args()

    settings = Settings(module=args.config)
    if args.port:
        settings.set("JSONRPC_PORT", [args.port])

    logging_config_path = settings.get("LOGGING_CONFIG")
    if logging_config_path and exists(logging_config_path):
        fileConfig(logging_config_path)
    else:
        logging.basicConfig(level=args.log_level)
        logger.setLevel(args.log_level)
        logger.addHandler(CONSOLE)

    worker = DBWorker(settings, args.no_batches, args.no_incoming, args.no_scoring)
    server = WorkerJsonRpcService(worker, settings)
    server.start_listening()
    worker.run()

