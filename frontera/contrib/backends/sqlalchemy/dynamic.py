import calendar

from datetime import datetime, timedelta

from frontera.contrib.backends.sqlalchemy.revisiting import Backend as RevisitingBackend, RevisitingQueue
from frontera.contrib.backends.sqlalchemy.components import retry_and_rollback
from frontera.contrib.backends.sqlalchemy.models import QueueModelMixin, DeclarativeBase
from frontera.core.components import Queue as BaseQueue, States, Partitioner

from sqlalchemy import Column, Integer, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import between


def utcnow_timestamp():
    d = datetime.utcnow()
    return calendar.timegm(d.timetuple())


class DynamicPartitioner(Partitioner):
    def __init__(self, partitioner):
        super(DynamicPartitioner, self).__init__(partitioner.partitions)
        self.partitioner = partitioner

    def partition(self, key):
        if not key:
            return 0
        return self.hash(key)

    def hash(self, key):
        return self.partitioner.hash(key) & self.HASH_MASK

    def get_key(self, request):
        return self.partitioner.get_key(request)


class DynamicQueueModel(QueueModelMixin, DeclarativeBase):
    __tablename__ = 'dynamic_queue'

    crawl_at = Column(BigInteger, nullable=False)
    # Redefine partition_id so it is not indexed.
    partition_id = Column(Integer)


class DynamicQueue(RevisitingQueue):
    def query_next_requests(self, max_n_requests, partition_id):
        return self.session.query(self.queue_model).\
            filter(DynamicQueueModel.crawl_at <= utcnow_timestamp(),
                   (DynamicQueueModel.partition_id % len(self.partitioner.partitions)) == partition_id).\
            order_by(DynamicQueueModel.score.desc(), DynamicQueueModel.crawl_at).\
            limit(max_n_requests)


class Backend(RevisitingBackend):
    def _create_queue(self, settings):
        return DynamicQueue(
            self.session_cls,
            DynamicQueueModel,
            DynamicPartitioner(self.partitioner),
            settings.get('SQLALCHEMYBACKEND_DEQUEUED_DELAY'))

