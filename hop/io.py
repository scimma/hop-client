from dataclasses import dataclass
from functools import lru_cache
from enum import Enum
import json
import logging
import random
import string
from typing import Union
import warnings

import pluggy

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from urllib.parse import urlparse

from .configure import get_config_path
from .auth import Auth
from .auth import load_auth
from .auth import select_matching_auth
from . import models
from . import plugins

logger = logging.getLogger("hop")


class StartPosition(Enum):
    EARLIEST = 1
    LATEST = 2

    def __str__(self):
        return self.name.lower()


def parse_kafka_url(val):
    """Extracts the group ID, broker addresses, and topic names from a Kafka URL.

        The URL should be in this form:
        ``kafka://[groupid@]broker[,broker2[,...]]/topic[,topic2[,...]]``

        The returned group ID and topic may be None if they aren't in the URL.

        """
    parsed = urlparse(val)
    if parsed.scheme != "kafka":
        raise ValueError("invalid kafka URL: must start with 'kafka://'")

    split_netloc = parsed.netloc.split("@", maxsplit=1)
    if len(split_netloc) == 2:
        group_id = split_netloc[0]
        broker_addresses = split_netloc[1].split(",")
    else:
        group_id = None
        broker_addresses = split_netloc[0].split(",")

    topics = parsed.path.lstrip("/")
    if len(topics) == 0:
        split_topics = None
    else:
        split_topics = topics.split(",")
    return group_id, broker_addresses, split_topics


class Stream(object):
    """Defines an event stream.

    Sets up defaults used within the client so that when a
    stream connection is opened, it will use defaults specified here.

    Args:
        auth: A `bool` or :class:`Auth <hop.auth.Auth>` instance. Defaults to
            loading from :meth:`auth.load_auth <hop.auth.load_auth>` if set to
            True. To disable authentication, set to False.
        start_at: The message offset to start at in read mode. Defaults to LATEST.
        persist: Whether to listen to new messages forever or stop
            when EOS is received in read mode. Defaults to False.

    """

    def __init__(self, auth=True, start_at=StartPosition.LATEST, persist=False):
        self._auth = [auth] if isinstance(auth, Auth) else auth
        self.start_at = start_at
        self.persist = persist

    @property
    @lru_cache(maxsize=1)
    def auth(self):
        # configuration is disabled in adc-streaming by passing None,
        # so to provide a nicer interface, we allow boolean flags as well.
        # this also explicitly gets around a problem in setting
        # configuration to True by default in the convenience class `stream`
        # which is set to `Stream()`. instead, configuration is first loaded
        # during the first open stream and cached for future use.
        if isinstance(self._auth, bool):
            if self._auth:
                try:
                    return load_auth()
                except FileNotFoundError:
                    logger.error(
                        "configuration set to True and configuration file "
                        f"not found at {get_config_path('auth')} to authenticate"
                    )
                    raise
            else:
                return None
        else:
            return self._auth

    def open(self, url, mode="r", group_id=None, **kwargs):
        """Opens a connection to an event stream.

        Args:
            url: Sets the broker URL to connect to.
            mode: Read ('r') or write ('w') from the stream.
            group_id: The consumer group ID from which to read.
                      Generated automatically if not specified.

        Returns:
            An open connection to the client, either a :class:`Producer` instance
            in write mode or a :class:`Consumer` instance in read mode.

        Raises:
            ValueError: If the mode is not set to read/write, if more than
                one topic is specified in write mode, or if more than one broker is specified

        """
        username, broker_addresses, topics = parse_kafka_url(url)
        if len(broker_addresses) > 1:
            raise ValueError("Multiple broker addresses are not supported")
        logger.debug("connecting to addresses=%s  username=%s  topics=%s",
                     broker_addresses, group_id, topics)

        if topics is None:
            raise ValueError("no topic(s) specified in kafka URL")

        if self.auth is not None:
            credential = select_matching_auth(self.auth, broker_addresses[0], username)
        else:
            credential = None

        if mode == "w":
            if len(topics) != 1:
                raise ValueError("must specify exactly one topic in write mode")
            if group_id is not None:
                warnings.warn("group ID has no effect when opening a stream in write mode")
            return Producer(broker_addresses, topics[0], auth=credential, **kwargs)
        elif mode == "r":
            if group_id is None:
                username = credential.username if credential is not None else None
                group_id = _generate_group_id(username, 10)
                logger.info(f"group ID not specified, generating a random group ID: {group_id}")
            return Consumer(
                group_id,
                broker_addresses,
                topics,
                start_at=self.start_at,
                auth=credential,
                read_forever=self.persist,
                **kwargs
            )
        else:
            raise ValueError("mode must be either 'w' or 'r'")


def _load_deserializer_plugins():
    """Load all registered deserializer plugins.

    """
    # set up plugin manager
    manager = pluggy.PluginManager("hop")
    manager.add_hookspecs(plugins)

    # load in base models
    manager.register(models)

    # load external models
    try:
        manager.load_setuptools_entrypoints("hop_plugin")
    except Exception:
        logger.warning(
            "Could not load external message plugins as one or more plugins "
            "generated errors upon import. to fix this issue, uninstall or fix "
            "any problematic plugins that are currently installed."
        )
        import sys
        import traceback
        traceback.print_exc(file=sys.stderr)

    # add all registered plugins to registry
    registered = {}
    for model_plugins in manager.hook.get_models():
        for name, model in model_plugins.items():
            plugin_name = name.upper()
            if plugin_name in registered:
                logger.warning(
                    f"Identified duplicate message plugin {plugin_name} registered under "
                    "the same name. this may cause unexpected behavior when using this "
                    "message format."
                )
            registered[plugin_name] = model

    return registered


class _DeserializerMixin:
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
                return content
            elif format in cls.__members__:
                return cls[format].value(**content)
            else:
                logger.warning(f"Message format {format} not recognized, returning a Blob")
                return models.Blob(content=content, missing_schema=True)

    def load(self, input_):
        return self.value.load(input_)

    def load_file(self, input_file):
        return self.value.load_file(input_file)


Deserializer = Enum(
    "Deserializer",
    _load_deserializer_plugins(),
    module=__name__,
    type=_DeserializerMixin
)


def _generate_group_id(user, n):
    """Generate a random Kafka group ID.

    Args:
        user: Username associated with the credential being used
        n: Length of randomly generated string suffix.

    Returns:
        The generated group ID.

    """
    alphanum = string.ascii_uppercase + string.digits
    rand_str = ''.join(random.SystemRandom().choice(alphanum) for _ in range(n))
    if user is None:
        return rand_str
    return '-'.join((user, rand_str))


@dataclass(frozen=True)
class Metadata:
    """Broker-specific metadata that accompanies a consumed message.

    """

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: Union[str, bytes]
    _raw: object

    @classmethod
    def from_message(cls, msg) -> 'Metadata':
        return cls(
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            timestamp=msg.timestamp,
            key=msg.key,
            _raw=msg,
        )


class Consumer:
    """
    An event stream opened for reading one or more topics.
    Instances of this class should be obtained from :meth:`Stream.open`.
    """

    def __init__(self, group_id, broker_addresses, topics, **kwargs):
        """
        Args:
            group_id: The Kafka consumer group to join for reading messages.
            broker_addresses: The list of bootstrap Kafka broker URLs.
            topics: The list of names of topics to which to subscribe.
            read_forever: If true, keep the stream open to wait for more messages
                after reading the last currently available message.
            start_at: The position in the topic stream at which to start
                reading, specified as a StartPosition object.
            auth: An Auth object specifying client authentication
                to use.
            error_callback: A callback which will be called with any
                confluent_kafka.KafkaError objects produced representing internal
                Kafka errors.
            offset_commit_interval: A datetime.timedelta specifying how often to
                report progress to Kafka.

        :meta private:
        """
        config = {}
        if group_id is not None:
            config["group_id"] = group_id
        # kafka-python requires explicit ports in addresses
        addresses_with_ports = []
        default_port_str = ":9092"
        for broker in broker_addresses:
            if ':' not in broker:
                addresses_with_ports.append(broker + default_port_str)
            else:
                addresses_with_ports.append(broker)
        config["bootstrap_servers"] = addresses_with_ports
        if "auth" in kwargs:
            if kwargs["auth"] is not None:
                config.update(kwargs["auth"]())
            del kwargs["auth"]
        if "read_forever" in kwargs:
            self.read_forever = kwargs["read_forever"]
            if self.read_forever:
                # standard value to never timeout
                config["consumer_timeout_ms"] = float('inf')
            else:
                # wait at most this long for more messages to arrive
                config["consumer_timeout_ms"] = 10000
            del kwargs["read_forever"]
        if "start_at" in kwargs:
            start_at = kwargs["start_at"]
            if start_at == StartPosition.EARLIEST:
                config["auto_offset_reset"] = "earliest"
            elif start_at == StartPosition.LATEST:
                config["auto_offset_reset"] = "latest"
            del kwargs["start_at"]
        if "error_callback" in kwargs:
            logger.warning(f"error_callback is not currently supported in Consumer")
            del kwargs["error_callback"]
        if "offset_commit_interval" in kwargs:
            ms = int(kwargs["offset_commit_interval"].total_seconds() * 1000)
            config["auto_commit_interval_ms"] = ms
            del kwargs["offset_commit_interval"]

        config["api_version"] = (1, 0)

        config.update(kwargs)

        self.topics = topics
        self._consumer = KafkaConsumer(*topics, **config)

    def read(self, metadata=False, autocommit=True, **kwargs):
        """Read messages from a stream.

        Args:
            metadata: Whether to receive message metadata alongside messages.
            autocommit: Whether messages are automatically marked as handled
                via `mark_done` when the next message is yielded. Defaults to True.
            batch_size: The number of messages to request from Kafka at a time.
                Lower numbers can give lower latency, while higher numbers will
                be more efficient, but may add latency.
            batch_timeout: The period of time to wait to get a full batch of
                messages from Kafka. Similar to batch_size, lower numbers can
                reduce latency while higher numbers can be more efficient at the
                cost of greater latency.
                If specified, this argument should be a datetime.timedelta
                object.
        """
        partitions = []
        ends = []
        got_partitions = False

        def get_partitions():
            nonlocal partitions, got_partitions, ends
            if got_partitions:
                return
            partitions = set(self._consumer.assignment())
            ends = self._consumer.end_offsets(partitions)
            # We may already be at the ends of some partitions, and we need to
            # filter them out up front, since we will never read a message from
            # them (in non-read_forever mode), so we won't otherwise discover
            # that we're done with them
            for partition in ends.keys():
                if self._consumer.position(partition) == ends[partition]:
                    partitions.discard(partition)
            got_partitions = True

        def update_partitions(message):
            nonlocal partitions, ends
            partition = TopicPartition(message.topic, message.partition)
            if partition in ends and message.offset + 1 == ends[partition]:
                partitions.discard(partition)
            return len(partitions) == 0

        # call poll once to force partition assignements to become known,
        # using a minimal timeout to avoid blocking in case we turn out to
        # already be at the end of all partitions
        data = self._consumer.poll(timeout_ms=0, max_records=1)
        for topic, messages in data:
            for message in messages:
                yield self._unpack(message, metadata=metadata)
                if autocommit:
                    self._consumer.commit_async()
                if not self.read_forever:
                    # just call update_partitions without considering breaking
                    # out of the loop beacuse we should definitely yield all
                    # mesasges we already have
                    update_partitions(message)

        # after poll, we should be able to get real partition data
        get_partitions()
        # and we may have also have already consumed everything
        if not self.read_forever and len(partitions) == 0:
            return

        for message in self._consumer:
            yield self._unpack(message, metadata=metadata)
            if autocommit:
                self._consumer.commit_async()
            if not self.read_forever and update_partitions(message):
                break

    @staticmethod
    def _unpack(message, metadata=False):
        """Deserialize and unpack messages.

        Args:
            message: The message to deserialize and unpack.
            metadata: Whether to receive message metadata alongside messages.
        """
        payload = json.loads(message.value.decode("utf-8"))
        payload = Deserializer.deserialize(payload)
        if metadata:
            return (payload, Metadata.from_message(message))
        else:
            return payload

    def mark_done(self, metadata):
        """Mark a message as fully-processed.

        Args:
            metadata: A Metadata instance containing broker-specific metadata.

        """
        self._consumer.mark_done(metadata._raw)

    def close(self):
        """End all subscriptions and shut down.

        """
        self._consumer.close()

    def __iter__(self):
        yield from self.read()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Producer:
    """
    An event stream opened for writing to a topic.
    Instances of this class should be obtained from :meth:`Stream.open`.
    """

    def __init__(self, broker_addresses, topic, **kwargs):
        """
        Args:
            broker_addresses: The list of bootstrap Kafka broker URLs.
            topic: The name of the topic to which to write.
            auth: An adc.auth.SASLAuth object specifying client authentication
                to use.
            delivery_callback: A callback which will be called when each message
                is either delivered or permenantly fails to be delivered.
            error_callback: A callback which will be called with any
                confluent_kafka.KafkaError objects produced representing internal
                Kafka errors.
            produce_timeout: A datetime.timedelta object specifying the maximum
                time to wait for a message to be sent to Kafka. If zero, sending
                will never time out.

        :meta private:
        """
        config = {}
        # kafka-python requires explicit ports in addresses
        addresses_with_ports = []
        default_port_str = ":9092"
        for broker in broker_addresses:
            if ':' not in broker:
                addresses_with_ports.append(broker + default_port_str)
            else:
                addresses_with_ports.append(broker)

        config["bootstrap_servers"] = addresses_with_ports
        if "auth" in kwargs:
            if kwargs["auth"] is not None:
                config.update(kwargs["auth"]())
            del kwargs["auth"]
        # TODO: set `acks`; very important for robustness/throughput!
        # TODO: set retries (does something only when acks!=0)
        # TODO: set batch_size (important for robustness/throughput)
        # TODO: set linger_ms (important for robustness/throughput)

        config.update(kwargs)

        self._producer = KafkaProducer(**config)
        self._topic = topic

    def write(self, message, headers=None):
        """Write messages to a stream.

        Args:
            message: The message to write.

        """
        if type(headers) == dict:
            headers = list(headers.items())
        return self._producer.send(self._topic, self._pack(message), headers=headers)

    @staticmethod
    def _pack(message):
        """Pack and serialize messages.

        Args:
            message: The message to pack and serialize.

        :meta private:
        """
        try:
            payload = message.serialize()
        except AttributeError:
            payload = {"format": "blob", "content": message}
        try:
            return json.dumps(payload).encode("utf-8")
        except TypeError:
            raise TypeError("Unable to pack a message of type "
                            + message.__class__.__name__
                            + " which cannot be serialized to JSON")

    def close(self):
        """Wait for enqueued messages to be written and shut down.

        """
        return self._producer.close()

    def flush(self, timeout=None):
        self._producer.flush(timeout)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return self.close()
