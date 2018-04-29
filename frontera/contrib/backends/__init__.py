# -*- coding: utf-8 -*-
from __future__ import absolute_import
from collections import OrderedDict

from frontera import Backend
from frontera.core.components import States


BATCH_MODE_ALL_PARTITIONS = 'ALL_PARTITION'


class CommonBackend(Backend):
    """
    A simpliest possible backend, performing one-time crawl: if page was crawled once, it will not be crawled again.
    """
    component_name = 'Common Backend'

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        self.metadata.frontier_start()
        self.queue.frontier_start()
        self.states.frontier_start()
        self.queue_size = self.queue.count()
        self.overused_batch_delay = self.manager.settings.get('OVERUSED_BATCH_DELAY', 5)
        self.overused = {}

    def frontier_stop(self):
        self.metadata.frontier_stop()
        self.queue.frontier_stop()
        self.states.frontier_stop()

    def add_seeds(self, seeds):
        for seed in seeds:
            seed.meta[b'depth'] = 0
        self.metadata.add_seeds(seeds)
        self.states.fetch([seed.meta[b'fingerprint'] for seed in seeds])
        self.states.set_states(seeds)
        self._schedule(seeds)
        self.states.update_cache(seeds)

    def on_new_batch(self, partitions):
        pass

    def _schedule(self, requests):
        batch = []
        queue_incr = 0
        for request in requests:
            schedule = True if request.meta[b'state'] in [States.NOT_CRAWLED, States.ERROR, None] else False
            batch.append((request.meta[b'fingerprint'], self._get_score(request), request, schedule))
            if schedule:
                queue_incr += 1
                request.meta[b'state'] = States.QUEUED
        self.queue.schedule(batch)
        self.metadata.update_score(batch)
        self.queue_size += queue_incr

    def _get_score(self, obj):
        return obj.meta.get(b'score', 1.0)

    def get_next_requests(self, max_next_requests, **kwargs):
        partitions = kwargs.pop('partitions', [0])  # TODO: Collect from all known partitions
        batch = []
        overused = self.get_overused_for_batch(partitions)
        if getattr(self.queue, 'batch_mode', None) == BATCH_MODE_ALL_PARTITIONS:
            batch.extend(self.queue.get_next_requests(max_next_requests, partitions, overused=overused, **kwargs))
        else:
            for partition_id in partitions:
                batch.extend(self.queue.get_next_requests(max_next_requests, partition_id, overused=overused[partition_id], **kwargs))
        self.queue_size -= len(batch)
        return batch

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED
        self.states.update_cache(response)
        self.metadata.page_crawled(response)

    def links_extracted(self, request, links):
        to_fetch = OrderedDict()
        for link in links:
            to_fetch[link.meta[b'fingerprint']] = link
            link.meta[b'depth'] = request.meta.get(b'depth', 0)+1
        self.states.fetch(to_fetch.keys())
        self.states.set_states(links)
        unique_links = to_fetch.values()
        self.metadata.links_extracted(request, unique_links)
        self._schedule(unique_links)
        self.states.update_cache(unique_links)

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR
        self.metadata.request_error(request, error)
        self.states.update_cache(request)

    def set_overused(self, partition_id, netlocs):
        new_overused = {
            netloc: self.overused_batch_delay
            for netloc in netlocs
        }
        new_overused.update(self.overused.get(partition_id, {}))
        self.overused[partition_id] = new_overused

    def get_overused_for_batch(self, partitions):
        overuseds = {}
        for partition in partitions:
            overuseds[partition] = set()
            expired = []
            for netloc in self.overused.get(partition, {}):
                if self.overused[partition][netloc] <= 0:
                    expired.append(netloc)
                else:
                    overuseds[partition].add(netloc)
                self.overused[partition][netloc] -= 1
            for netloc in expired:
                del self.overused[partition][netloc]
        return overuseds

    def finished(self):
        return self.queue_size == 0
