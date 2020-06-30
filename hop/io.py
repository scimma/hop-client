#!/usr/bin/env python

__author__ = "Patrick Godwin (patrick.godwin@psu.edu)"
__description__ = "a module for i/o utilities"

from contextlib import contextmanager
from dataclasses import dataclass
import getpass
from enum import Enum
import json
import logging
import random
import string
from typing import Union
import warnings

from adc import consumer, errors, kafka, producer

from . import models

logger = logging.getLogger("hop")

StartPosition = consumer.ConsumerStartPosition


class Stream(object):
    """Defines an event stream.

    Sets up defaults used within the client so that when a
    stream connection is opened, it will use defaults specified here.

    Args:
        auth: An Auth instance.
        start_at: The message offset to start at.
        persist: Whether to listen to new messages forever or stop
             when EOS is received.

    """

    def __init__(self, auth=None, start_at=None, persist=None):
        self.auth = auth
        self.start_at = start_at
        self.persist = persist

    def open(self, url, mode="r", auth=None, start_at=None, persist=None, metadata=False):
        """Opens a connection to an event stream.

        Args:
            url: Sets the broker URL to connect to.

        Kwargs:
            mode: Read ('r') or write ('w') from the stream.
            auth: An Auth instance.
            start_at: The message offset to start at (read only).
            persist: Whether to listen to new messages forever or stop
                 when EOS is received (read only).
            metadata: Whether to receive message metadata along
                with payload (read only).

        Returns:
            An open connection to the client, either an adc Producer instance
            in write mode or an adc Consumer instance in read mode.

        Raises:
            ValueError: If the mode is not set to read/write or if more than
                one topic is specified in write mode.

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
                group_id = _generate_group_id(10)
                logger.info(f"group ID not specified, generating a random group ID: {group_id}")
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
        """Deserialize a stream message and instantiate a model.

        Args:
            message: A serialized message.

        Returns:
            A data container corresponding to the format in the
            serialized message.

        Raises:
            ValueError: If the message is incorrectly formatted or
                if the message format is not recognized.

        """
        try:
            format = message["format"].upper()
            content = message["content"]
        except (KeyError, TypeError):
            raise ValueError("Message is incorrectly formatted")
        else:
            if format == Deserializer.BLOB.name:
                return cls[format].value(content=content)
            elif format in cls.__members__:
                return cls[format].value(**content)
            else:
                logger.warning(f"Message format {format} not recognized, returning a MessageBlob")
                return models.MessageBlob(content=content, missing_schema=True)

    def load(self, input_):
        return self.value.load(input_)

    def load_file(self, input_file):
        return self.value.load_file(input_file)


def _generate_group_id(n):
    """Generate a random Kafka group ID.

    Args:
        n: Length of randomly generated string.

    Returns:
        The generated group ID.

    """
    alphanum = string.ascii_uppercase + string.digits
    rand_str = ''.join(random.SystemRandom().choice(alphanum) for _ in range(n))
    return '-'.join((getpass.getuser(), rand_str))


@dataclass
class _Metadata:
    """Broker-specific metadata that accompanies a consumed message.

    """

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: Union[str, bytes]


class _Consumer(consumer.Consumer):
    def stream(self, metadata=False, **kwargs):
        for message in super().stream(**kwargs):
            yield self.unpack(message, metadata=metadata)

    @staticmethod
    def unpack(message, metadata=False):
        payload = json.loads(message.value().decode("utf-8"))
        payload = Deserializer.deserialize(payload)
        if metadata:
            return (
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
            return payload


class _Producer(producer.Producer):
    def write(self, message):
        super().write(self.pack(message))

    @staticmethod
    def pack(message):
        try:
            payload = message.serialize()
        except AttributeError:
            payload = {"format": "blob", "content": message}
        return json.dumps(payload).encode("utf-8")


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
