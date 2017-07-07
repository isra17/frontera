import calendar

from datetime import datetime, timedelta

from frontera.contrib.backends.sqlalchemy.revisiting import Backend as RevisitingBackend, RevisitingQueue
from frontera.contrib.backends.sqlalchemy.components import retry_and_rollback
from frontera.contrib.backends.sqlalchemy.models import QueueModelMixin, DeclarativeBase
from frontera.core.components import Queue as BaseQueue, States, Partitioner
from frontera.contrib.backends import BATCH_MODE_ALL_PARTITIONS

from sqlalchemy import Column, Integer, BigInteger, func, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import between
from sqlalchemy.orm import with_expression, query_expression


def utcnow_timestamp():
    d = datetime.utcnow()
    return calendar.timegm(d.timetuple())


class DynamicPartitioner(Partitioner):
    def __init__(self, partitioner):
        self.partitioner = partitioner

    @property
    def partitions(self):
        return self.partitioner.partitions

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
    partition_id = query_expression()
    partition_seed = Column(Integer)


class DynamicQueue(RevisitingQueue):

    batch_mode = BATCH_MODE_ALL_PARTITIONS

    def query_next_requests(self, max_n_requests, partitions):
        partition = DynamicQueueModel.partition_seed % len(self.partitioner.partitions)

        partition_query = self.session.query(
                self.queue_model,
                func.row_number().over(
                    partition_by=partition,
                    order_by=[
                        self.queue_model.score.desc(),
                        self.queue_model.crawl_at
                    ]).label('rank')
            ).\
            filter(
                and_(
                    self.queue_model.crawl_at <= utcnow_timestamp(),
                    partition.in_(partitions)
                )
            ).\
            subquery()

        q = self.session.query(self.queue_model).\
                select_entity_from(partition_query).\
                options(
                    with_expression(self.queue_model.partition_id, partition)
                ).\
                filter(partition_query.c.rank <= max_n_requests)
        print(str(q))
        return q


class Backend(RevisitingBackend):
    def _create_queue(self, settings):
        return DynamicQueue(
            self.session_cls,
            DynamicQueueModel,
            DynamicPartitioner(self.partitioner),
            settings.get('SQLALCHEMYBACKEND_DEQUEUED_DELAY'))

