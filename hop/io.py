from dataclasses import dataclass
from functools import lru_cache
import getpass
from enum import Enum
import json
import logging
import random
import string
from typing import Union
import warnings

import confluent_kafka
import pluggy

from adc import consumer, errors, kafka, producer

from .configure import get_config_path
from .auth import load_auth
from . import models
from . import plugins

logger = logging.getLogger("hop")

StartPosition = consumer.ConsumerStartPosition


class Stream(object):
    """Defines an event stream.

    Sets up defaults used within the client so that when a
    stream connection is opened, it will use defaults specified here.

    Args:
        auth: A `bool` or `Auth` instance. Defaults to loading from `auth.load_auth()`
            if set to True. To disable authentication, set to False.
        start_at: The message offset to start at in read mode. Defaults to LATEST.
        persist: Whether to listen to new messages forever or stop
             when EOS is received in read mode. Defaults to False.

    """

    def __init__(self, auth=True, start_at=StartPosition.LATEST, persist=False):
        self._auth = auth
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
                        "configuration set to True and configuration file"
                        f"not found at {get_config_path()} to authenticate"
                    )
                    raise
            else:
                return None
        else:
            return self._auth

    def open(self, url, mode="r"):
        """Opens a connection to an event stream.

        Args:
            url: Sets the broker URL to connect to.

        Kwargs:
            mode: Read ('r') or write ('w') from the stream.

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

        if mode == "w":
            if len(topics) != 1:
                raise ValueError("must specify exactly one topic in write mode")
            if group_id is not None:
                warnings.warn("group ID has no effect when opening a stream in write mode")
            return _Producer(broker_addresses, topics[0], auth=self.auth)
        elif mode == "r":
            if group_id is None:
                group_id = _generate_group_id(10)
                logger.info(f"group ID not specified, generating a random group ID: {group_id}")
            return _Consumer(
                group_id,
                broker_addresses,
                topics,
                start_at=self.start_at,
                auth=self.auth,
                read_forever=self.persist,
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
            "could not load external message plugins as one or more plugins "
            "generated errors upon import. to fix this issue, uninstall or fix "
            "any problematic plugins that are currently installed."
        )

    # add all registered plugins to registry
    registered = {}
    for model_plugins in manager.hook.get_models():
        for name, model in model_plugins.items():
            plugin_name = name.upper()
            if plugin_name in registered:
                logger.warning(
                    f"identified duplicate message plugin {plugin_name} registered under "
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


@dataclass(frozen=True)
class Metadata:
    """Broker-specific metadata that accompanies a consumed message.

    """

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: Union[str, bytes]
    _raw: confluent_kafka.Message

    @classmethod
    def from_message(cls, msg: confluent_kafka.Message) -> 'Metadata':
        return cls(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            timestamp=msg.timestamp()[1],
            key=msg.key(),
            _raw=msg,
        )


class _Consumer:
    def __init__(self, group_id, broker_addresses, topics, **kwargs):
        self._consumer = consumer.Consumer(consumer.ConsumerConfig(
            broker_urls=broker_addresses,
            group_id=group_id,
            **kwargs,
        ))
        for t in topics:
            self._consumer.subscribe(t)

    def read(self, metadata=False, autocommit=True, **kwargs):
        """Read messages from a stream.

        Args:
            metadata: Whether to receive message metadata alongside messages.
            autocommit: Whether messages are automatically marked as handled
                via `mark_done` when the next message is yielded. Defaults to True.

        """
        for message in self._consumer.stream(autocommit=autocommit, **kwargs):
            yield self.unpack(message, metadata=metadata)

    @staticmethod
    def unpack(message, metadata=False):
        """Deserialize and unpack messages.

        Args:
            message: The message to deserialize and unpack.
            metadata: Whether to receive message metadata alongside messages.

        """
        payload = json.loads(message.value().decode("utf-8"))
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


class _Producer:
    def __init__(self, broker_addresses, topic, **kwargs):
        self._producer = producer.Producer(producer.ProducerConfig(
            broker_urls=broker_addresses,
            topic=topic,
            delivery_callback=errors.raise_delivery_errors,
            **kwargs,
        ))

    def write(self, message):
        """Write messages to a stream.

        Args:
            message: The message to write.

        """
        self._producer.write(self.pack(message))

    @staticmethod
    def pack(message):
        """Pack and serialize messages.

        Args:
            message: The message to pack and serialize.

        """
        try:
            payload = message.serialize()
        except AttributeError:
            payload = {"format": "blob", "content": message}
        return json.dumps(payload).encode("utf-8")

    def close(self):
        """Wait for enqueued messages to be written and shut down.

        """
        return self._producer.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return self._producer.__exit__(*exc)
