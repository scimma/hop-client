#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module for i/o utilities"

from collections import namedtuple
from contextlib import contextmanager
from enum import Enum
import json
import logging
import warnings

from adc import consumer, errors, kafka, producer

from . import models

logger = logging.getLogger("adc-streaming")

StartPosition = consumer.ConsumerStartPosition


class Stream(object):
    """Defines an event stream.

    Sets up defaults used within the client so that when a
    stream connection is opened, it will use defaults specified here.

    Args:
      auth: authentication options
      start_at: the message offset to start at
      persist: whether to listen to new messages forever or stop
               when EOS is received

    """

    def __init__(self, auth=None, start_at=None, persist=None):
        self.auth = auth
        self.start_at = start_at
        self.persist = persist

    def open(self, url, mode="r", auth=None, start_at=None, persist=None, metadata=False):
        """Opens a connection to an event stream.

        Args:
          url: sets the broker URL to connect to

        Kwargs:
          mode: read ('r') or write ('w') from the stream
          auth: authentication options
          start_at: the message offset to start at
          persist: whether to listen to new messages forever or stop
                   when EOS is received
          metadata: whether to receive message metadata along with payload

        """
        group_id, broker_addresses, topics = kafka.parse_kafka_url(url)
        logger.debug("connecting to addresses=%s  group_id=%s  topics=%s",
                     broker_addresses, group_id, topics)
        if not auth and self.auth:
            auth = self.auth
        if mode == "w":
            if len(topics) != 1:
                raise ValueError("must specify exactly one topic in write mode")
            if group_id is not None:
                warnings.warn("group ID has no effect when opening a stream in write mode")
            if start_at is not None:
                warnings.warn("start_at has no effect when opening a stream in write mode")
            if persist is not None:
                warnings.warn("read_forever has no effect when opening a stream in write mode")
            return _open_producer(broker_addresses, topics[0], auth=auth)
        elif mode == "r":
            if group_id is None:
                raise ValueError("group ID must be set when in reader mode")
            # set up extra options if provided
            opts = {}
            if start_at or self.start_at:
                opts["start_at"] = start_at if start_at else self.start_at
            if persist is not None or self.persist is not None:
                opts["read_forever"] = persist if persist is not None else self.persist
            return _open_consumer(
                group_id,
                broker_addresses,
                topics,
                auth=auth,
                metadata=metadata,
                **opts,
            )
        else:
            raise ValueError("mode must be either 'w' or 'r'")


class Deserializer(Enum):
    CIRCULAR = models.GCNCircular
    VOEVENT = models.VOEvent
    BLOB = models.MessageBlob

    @classmethod
    def deserialize(cls, message):
        try:
            format = message["format"]
            content = message["content"]
        except (KeyError, TypeError):
            return message
        else:
            return cls[format.upper()].value(**content)

    def load(self, input_):
        return self.value.load(input_)

    def load_file(self, input_file):
        return self.value.load_file(input_file)


_Metadata = namedtuple("Metadata", "topic partition offset timestamp key")


class _Consumer(consumer.Consumer):
    def stream(self, metadata=False, **kwargs):
        for message in super().stream(**kwargs):
            payload = json.loads(message.value().decode("utf-8"))
            payload = Deserializer.deserialize(payload)
            if metadata:
                yield (
                    payload,
                    _Metadata(
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        message.timestamp()[1],
                        message.key(),
                    )
                )
            else:
                yield payload


class _Producer(producer.Producer):
    def write(self, message):
        try:
            payload = message.serialize()
        except AttributeError:
            payload = {"format": "blob", "content": message}
        super().write(json.dumps(payload).encode("utf-8"))


@contextmanager
def _open_consumer(group_id, broker_addresses, topics, metadata=False, **kwargs):
    client = _Consumer(consumer.ConsumerConfig(
        broker_urls=broker_addresses,
        group_id=group_id,
        **kwargs,
    ))
    for t in topics:
        client.subscribe(t)
    try:
        yield client.stream(metadata=metadata)
    finally:
        client.close()


def _open_producer(broker_addresses, topic, **kwargs):
    return _Producer(producer.ProducerConfig(
        broker_urls=broker_addresses,
        topic=topic,
        delivery_callback=errors.raise_delivery_errors,
        **kwargs,
    ))
