import calendar

from datetime import datetime, timedelta

from frontera.contrib.backends.sqlalchemy.revisiting import Backend as RevisitingBackend, RevisitingQueue
from frontera.contrib.backends.sqlalchemy.components import retry_and_rollback
from frontera.contrib.backends.sqlalchemy.models import QueueModelMixin, DeclarativeBase
from frontera.core.components import Queue as BaseQueue, States, Partitioner
from frontera.contrib.backends import BATCH_MODE_ALL_PARTITIONS

from sqlalchemy import Column, Integer, BigInteger, func, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import between, text
from sqlalchemy.orm import with_expression, query_expression
from sqlalchemy.sql.functions import Function
from sqlalchemy.dialects.postgresql import array


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
    netloc = query_expression()
    partition_id = query_expression()
    partition_seed = Column(Integer, nullable=False)


class DynamicQueue(RevisitingQueue):

    batch_mode = BATCH_MODE_ALL_PARTITIONS

    def __init__(self, *args, score_window=1000, max_request_per_host=60):
        super(DynamicQueue, self).__init__(*args)
        self.score_window = score_window
        self.max_request_per_host = max_request_per_host


    def query_next_requests(self, max_n_requests, partitions):
        partitions_count = len(self.partitioner.partitions)
        score_window = partitions_count * self.score_window
        partition = DynamicQueueModel.partition_seed % partitions_count
        netloc = text("(regexp_matches(url, '^(?:://)?([^/]+)'))[1]")

        score_query = self.session.query(
                self.queue_model,
                func.row_number().over(
                    order_by=[
                        self.queue_model.score.desc(),
                        self.queue_model.crawl_at
                    ]).label('score_rank')
            ).\
            filter(
                and_(
                    self.queue_model.crawl_at <= utcnow_timestamp(),
                    partition.in_(partitions)
                )
            ).\
            limit(score_window).\
            subquery()

        netloc_query = self.session.query(
                self.queue_model,
                score_query.c.score_rank,
                func.row_number().over(
                    partition_by=[
                        partition,
                        netloc
                    ],
                    order_by=score_query.c.score_rank
                ).label('netloc_rank')
            ).\
            select_entity_from(score_query).\
            subquery()

        partition_query = self.session.query(
                self.queue_model,
                func.row_number().over(
                    partition_by=partition,
                    order_by=netloc_query.c.score_rank
                ).label('partition_rank')
            ).\
            select_entity_from(netloc_query).\
            filter(netloc_query.c.netloc_rank <= self.max_request_per_host).\
            subquery()

        return self.session.query(self.queue_model).\
                select_entity_from(partition_query).\
                options(
                    with_expression(self.queue_model.partition_id, partition)
                ).\
                filter(partition_query.c.partition_rank <= max_n_requests)


    def request_data(self, *args):
        data = super(DynamicQueue, self).request_data(*args)
        data['partition_seed'] = data.pop('partition_id')
        return data

class Backend(RevisitingBackend):
    def _create_queue(self, settings):
        return DynamicQueue(
            self.session_cls,
            DynamicQueueModel,
            DynamicPartitioner(self.partitioner),
            settings.get('SQLALCHEMYBACKEND_DEQUEUED_DELAY'),
            score_window=settings.get('SQLALCHEMYBACKEND_SCORE_WINDOW'),
            max_request_per_host=settings.get('SQLALCHEMYBACKEND_MAX_REQUEST_PER_HOST'))

