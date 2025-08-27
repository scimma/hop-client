from collections.abc import Collection, Mapping
import dataclasses
from datetime import timedelta
from functools import lru_cache
from enum import Enum
import json
import logging
import random
import string
import time
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse
import uuid
import warnings

import bson
import confluent_kafka
import pluggy
import requests

from adc import consumer, errors, kafka
from adc import producer as adc_producer
from confluent_kafka.admin import AdminClient, ConfigEntry, ConfigResource, ResourceType

from .configure import get_config_path, load_config
from .auth import Auth, AmbiguousCredentialError
from .auth import load_auth
from .auth import select_matching_auth
from . import http_scram
from . import models
from . import plugins

logger = logging.getLogger("hop")

StartPosition = consumer.ConsumerStartPosition


class Stream(object):
    """Defines an event stream.

    Sets up defaults used within the client so that when a
    stream connection is opened, it will use defaults specified here.

    Args:
        auth: A `bool` or :class:`Auth <hop.auth.Auth>` instance. Defaults to
            loading from :meth:`auth.load_auth <hop.auth.load_auth>` if set to
            True. To disable authentication, set to False.
        start_at: The message offset to start at in read mode. Defaults to LATEST.
        until_eos: Whether to listen to new messages forever (False) or stop
            when EOS is received in read mode (True). Defaults to False.

    """

    def __init__(self, auth=True, start_at=StartPosition.LATEST, until_eos=False):
        self._auth = [auth] if isinstance(auth, Auth) else auth
        self.start_at = start_at
        self.until_eos = until_eos

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

    @property
    @lru_cache(maxsize=1)
    def config(self):
        # Note that we return and cache the Config object, not its dictionary form, to ensure that
        # when the dictionary form is used, it is a new dictionary each time, so that modifications
        # do not leak back into the cached data.
        return load_config()

    def open(self, url, mode="r", group_id=None, ignoretest=True,
             produce_timeout=timedelta(seconds=0.0),
             **kwargs):
        """Opens a connection to an event stream.

        Args:
            url: Sets the broker URL to connect to.
            mode: Read ('r') or write ('w') from the stream.
            group_id: The consumer group ID from which to read.
                      Generated automatically if not specified.
            ignoretest: When True, read mode will silently discard
                        test messages.
            produce_timeout: A limit on the time within which each
                             published message must be sent. If zero,
                             no limit is applied, and closing the
                             producer will wait indefinitely for all
                             queued messages to send.

        Returns:
            An open connection to the client, either a :class:`Producer` instance
            in write mode or a :class:`Consumer` instance in read mode.

        Raises:
            ValueError: If the mode is not set to read/write, if no topic
                is specified in read mode, or if more than one broker is specified

        """
        username, broker_addresses, topics = kafka.parse_kafka_url(url)
        if len(broker_addresses) > 1:
            raise ValueError("Multiple broker addresses are not supported")
        logger.debug("connecting to addresses=%s  username=%s  topics=%s",
                     broker_addresses, group_id, topics)

        if self.auth is not None:
            try:
                credential = select_matching_auth(self.auth, broker_addresses[0], username)
            except AmbiguousCredentialError as err:
                msg = err.message
                msg += "\nTo select a specific credential to use, embed its username in the URL:"
                msg += f"\n  kafka://<username>@{broker_addresses[0]}/{','.join(topics)}"
                raise AmbiguousCredentialError(msg)
        else:
            credential = None

        # fetch default configuration, but write kwargs on top of it, so that anything explicitly
        # programmatically specified takes precedence
        config_dict = self.config.asdict()
        config_dict.update(kwargs)
        kwargs = config_dict

        if mode == "w":
            if topics is None:
                topics = []
            if group_id is not None:
                warnings.warn("group ID has no effect when opening a stream in write mode")
            return Producer(broker_addresses, topics, auth=credential,
                            produce_timeout=produce_timeout, **kwargs)
        elif mode == "r":
            if topics is None or len(topics) == 0:
                raise ValueError("no topic(s) specified in kafka URL")
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
                read_forever=not self.until_eos,
                ignoretest=ignoretest,
                **kwargs,
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

        def from_format(data, format, deserialize=True):
            format = format.upper()
            if format in cls.__members__:
                if deserialize:
                    return cls[format].value.deserialize(data)
                else:
                    return cls[format].value(**data)
            else:
                logger.warning(f"Message format {format} not recognized; returning a Blob")
                logger.warning(f"Known message formats are {cls.__members__}")
                return models.Blob(content=data)

        # first look for a format header
        if message.headers() is not None:
            try:
                format_header = filter(lambda h: h[0] == "_format", message.headers())
                format = next(format_header)[1].decode("utf-8")
                return from_format(message.value(), format)
            except StopIteration:  # no format header
                pass
        # otherwise, try doing old-style JSON envelope decoding
        try:
            old = json.loads(message.value().decode("utf-8"))
            if isinstance(old, Mapping) and "format" in old and "content" in old:
                if old["format"] == "blob":  # this was the old label for JSON
                    return models.JSONBlob(content=old["content"])
                return from_format(old["content"], old["format"], deserialize=False)
            # not labeled according to our scheme, but it is valid JSON
            return models.JSONBlob(content=old)
        # if we can't tell what the data is, pass it on unchanged
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.info("Unknown message format; returning a Blob")
            return models.Blob(content=message.value())

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


@dataclasses.dataclass(frozen=True)
class Metadata:
    """Broker-specific metadata that accompanies a consumed message.

    """

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: Union[str, bytes]
    headers: List[Tuple[str, bytes]]
    _raw: confluent_kafka.Message

    @classmethod
    def from_message(cls, msg: confluent_kafka.Message) -> 'Metadata':
        return cls(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            timestamp=msg.timestamp()[1],
            key=msg.key(),
            headers=msg.headers(),
            _raw=msg,
        )


def _get_offload_server(broker_addresses: List[str], auth: Optional[Auth]):
    """Find out the large message offload server, if any, associated with a broker or set of
    brokers.

    Args:
        broker_addresses: The list of bootstrap Kafka broker URLs.
        auth: The credential, if any to use for authentication with the broker(s)

    Returns: The URL of the offload server, or None if the broker does not report one
    """

    logger.debug(f"Looking up message offload endpoint for {broker_addresses}")
    username = auth.username if auth is not None else None
    # This always uses a random group ID because we always want to perform
    # a read regardless of what other clients are doing.
    group_id = _generate_group_id(username, 10)
    config = {
        "auth": auth,
        "broker_urls": broker_addresses,
        "error_callback": errors.raise_delivery_errors,
        "group_id": group_id,
        # There should be only one message, and we want to read it, not
        # wait for a new one, so we always seek to the earliest offset.
        "start_at": StartPosition.EARLIEST,
        "read_forever": False,
    }
    mconsumer = consumer.Consumer(consumer.ConsumerConfig(**config))
    raw_metadata = None
    # For now, the large message offload address is the only thing stored in the metadata.
    # In future, we might have other things, and this function should evolve to be a general
    # metadata utility.
    lmo_endpoint = None  # Assume not available until proven otherwise
    try:
        mconsumer.subscribe("sys.metadata")
    except ValueError:
        logger.debug(f"Failed to subscribe to topic sys.metadata on {broker_addresses}")
        mconsumer.close()
    else:
        try:
            for metamsg in mconsumer.stream():
                raw_metadata = metamsg.value()
        finally:
            mconsumer.close()
    if raw_metadata is not None:
        try:
            metadata = json.loads(raw_metadata.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        else:
            if isinstance(metadata, Mapping) and "LargeMessageUploadEndpoint" in metadata:
                if isinstance(metadata["LargeMessageUploadEndpoint"], str):
                    lmo_endpoint = metadata["LargeMessageUploadEndpoint"]
    else:
        logger.debug(f"Got no data from sys.metadata on {broker_addresses}")
    if lmo_endpoint and urlparse(lmo_endpoint).scheme == "http":
        warnings.warn("Large message offload service is bare HTTP; man-in-the-middle attacks "
                      "could exploit credentials used with it.")
    return lmo_endpoint


def _http_error_to_kafka(status: int, msg: str = ""):
    err_code = confluent_kafka.KafkaError.UNKNOWN
    if status == 400:
        err_code = confluent_kafka.KafkaError.INVALID_REQUEST
    elif status == 401:
        err_code = confluent_kafka.KafkaError.SASL_AUTHENTICATION_FAILED
    elif status == 403:
        err_code = confluent_kafka.KafkaError.TOPIC_AUTHORIZATION_FAILED
    elif status == 404:
        err_code = confluent_kafka.KafkaError.RESOURCE_NOT_FOUND
    elif status == 413:
        err_code = confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE
    elif status >= 400 and status < 500:
        err_code = confluent_kafka.KafkaError.INVALID_REQUEST
    elif status >= 502 and status <= 504:
        err_code = confluent_kafka.KafkaError.NETWORK_EXCEPTION
    elif status == 505:
        err_code = confluent_kafka.KafkaError.UNSUPPORTED_VERSION
    fatal = False
    retriable = err_code == confluent_kafka.KafkaError.NETWORK_EXCEPTION
    txn_requires_abort = False
    return confluent_kafka.KafkaError(err_code, msg, fatal, retriable, txn_requires_abort)


def _filter_valid_args(cls, kwargs: dict):
    """
    Extract from a dictionary the subset of its entries which are valid arguments to the
    constructor of the given dataclass.

    Args:
        cls: The target class which must be a dataclass
        kwargs: The argument dictionary to be filtered
    Return: A dictionary which is safe to pass to cls()
    """
    filtered = {}
    for field in dataclasses.fields(cls):
        if field.name in kwargs:
            filtered[field.name] = kwargs[field.name]
    return filtered


class Consumer:
    """
    An event stream opened for reading one or more topics.
    Instances of this class should be obtained from :meth:`Stream.open`.
    """

    def __init__(self, group_id, broker_addresses, topics, ignoretest=True,
                 fetch_external: bool = True, **kwargs):
        """
        Args:
            group_id: The Kafka consumer group to join for reading messages.
            broker_addresses: The list of bootstrap Kafka broker URLs.
            topics: The list of names of topics to which to subscribe.
            read_forever: If true, keep the stream open to wait for more messages
                after reading the last currently available message.
            start_at: The position in the topic stream at which to start
                reading, specified as a StartPosition object.
            auth: An adc.auth.SASLAuth object specifying client authentication
                to use.
            error_callback: A callback which will be called with any
                confluent_kafka.KafkaError objects produced representing internal
                Kafka errors.
            offset_commit_interval: A datetime.timedelta specifying how often to
                report progress to Kafka.
            ignoretest: When True, ignore test messages. When False, process them
                normally.
            fetch_external: When true, automatically download data referred to by
                            'External' messages, and return it in place of the
                            external message itself.

        :meta private:
        """
        if isinstance(broker_addresses, str):
            broker_addresses = [broker_addresses]
        logger.info(f"connecting to kafka://{','.join(broker_addresses)}")
        self._conf = consumer.ConsumerConfig(
            broker_urls=broker_addresses,
            group_id=group_id,
            **_filter_valid_args(consumer.ConsumerConfig, kwargs),
        )
        self._consumer = consumer.Consumer(self._conf)
        logger.info(f"subscribing to topics: {topics}")
        self._consumer.subscribe(topics)
        self.ignoretest = ignoretest
        self.fetch_external = fetch_external
        self.auth = kwargs["auth"] if "auth" in kwargs else None
        self.broker_addresses = broker_addresses

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
        logger.info("processing messages from stream")
        for message in self._consumer.stream(autocommit=autocommit, **kwargs):
            if self.ignoretest and self.is_test(message):
                continue
            yield self._unpack(message, metadata=metadata)
        logger.info("finished processing messages")

    def _unpack(self, message, metadata=False):
        """Deserialize and unpack messages.

        Args:
            message: The message to deserialize and unpack.
            metadata: Whether to receive message metadata alongside messages.
        """
        payload = Deserializer.deserialize(message)
        if isinstance(payload, models.ExternalMessage) and self.fetch_external:
            return self._fetch_external(payload.url, metadata, message)
        if metadata:
            return (payload, Metadata.from_message(message))
        else:
            return payload

    def read_raw(self, metadata=False, autocommit=True, **kwargs):
        """Read messages from a stream without applying any deserialization.

        This is an advanced interface; for most purposes it is preferable to use
        :meth:`Consumer.read <hop.io.Consumer.read>` instead.

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
        logger.info("processing messages from stream")
        for message in self._consumer.stream(autocommit=autocommit, **kwargs):
            if self.ignoretest and self.is_test(message):
                continue
            payload = message.value()
            if metadata:
                yield (payload, Metadata.from_message(message))
            else:
                yield payload
        logger.info("finished processing messages")

    class ExternalMessage:
        def __init__(self, data: bytes, headers: List[Tuple[str, bytes]], topic, partition, offset,
                     timestamp: int, key, error: Optional[confluent_kafka.KafkaError] = None):
            self._data = data
            self._headers = [tuple(header) for header in headers]
            self._topic = topic
            self._partition = partition
            self._offset = offset
            self._timestamp = timestamp
            self._key = key
            self._error = error

        @classmethod
        def make_error(cls, error):
            return cls(b"", [], "", 0, 0, 0, "", error)

        def value(self):
            return self._data

        def headers(self):
            return self._headers

        def topic(self):
            return self._topic

        def partition(self):
            return self._partition

        def offset(self):
            return self._offset

        def timestamp(self):
            # We don't actually know the timestamp type. Does anyone care?
            return (confluent_kafka.TIMESTAMP_CREATE_TIME, self._timestamp)

        def key(self):
            return self._key

        def error(self):
            return self._error

    def _fetch_external(self, url: str, metadata: bool, parent_msg):
        logger.debug(f"Attempting to fetch external message payload from {url}")
        auth = None
        # We don't want to use our credential with just any server; while the server cannot
        # steal it, it does get the opportunity to act with our authority, which a malicious
        # server could abuse. Therefore, we authenticate only if the server is the one specifically
        # associated with the broker (according to the broker, which we trust).
        if self.auth is not None:
            try:
                # check if we have this information cached
                trusted_offload_url = getattr(self, "trusted_offload_url")
            except AttributeError:
                # if not, load metadata from the broker
                self.trusted_offload_url = urlparse(_get_offload_server(self.broker_addresses,
                                                                        self.auth))
                trusted_offload_url = self.trusted_offload_url
            parsed = urlparse(url)
            if trusted_offload_url is not None and \
                    parsed.scheme == trusted_offload_url.scheme and \
                    parsed.netloc == trusted_offload_url.netloc:
                auth = http_scram.SCRAMAuth(self.auth, shortcut=True)
                logger.debug(" Will send auth info in HTTP request")
        resp = requests.get(url, auth=auth)
        if not resp.ok:
            err = _http_error_to_kafka(resp.status_code, f"Failed to fetch message data from {url}:"
                                       f" HTTP Error {resp.status_code}")
            if self._conf.error_callback is not None:
                self._conf.error_callback(err)
            return self.ExternalMessage.make_error(err)
        try:
            decoded = bson.loads(resp.content)
        except Exception:
            err = confluent_kafka.KafkaError(confluent_kafka.KafkaError._VALUE_DESERIALIZATION,
                                             f"Message data from {url} is not valid BSON",
                                             False, False, False)
            if self._conf.error_callback is not None:
                self._conf.error_callback(err)
            return self.ExternalMessage.make_error(err)
        err_msg = self._check_bson_message_structure(decoded, url)
        if err_msg is not None:
            err = confluent_kafka.KafkaError(confluent_kafka.KafkaError._VALUE_DESERIALIZATION,
                                             err_msg, False, False, False)
            if self._conf.error_callback is not None:
                self._conf.error_callback(err)
            return self.ExternalMessage.make_error(err)
        message = self.ExternalMessage(data=decoded["message"],
                                       headers=decoded["metadata"]["headers"],
                                       topic=parent_msg.topic(),
                                       partition=parent_msg.partition(),
                                       offset=parent_msg.offset(),
                                       timestamp=decoded["metadata"]["timestamp"],
                                       key=decoded["metadata"].get("key", None),
                                       )
        payload = Deserializer.deserialize(message)
        if metadata:
            return (payload, Metadata.from_message(message))
        else:
            return payload

    @staticmethod
    def _check_bson_message_structure(decoded, url):
        if not isinstance(decoded, Mapping):
            return f"Message data from {url} is not a mapping"
        if "message" not in decoded or not isinstance(decoded["message"], bytes):
            return f"Original message data not present in message data from {url}"
        if "metadata" not in decoded or not isinstance(decoded["metadata"], Mapping):
            return f"Metadata not present or malformed in message data from {url}"
        if "headers" not in decoded["metadata"] or \
                not isinstance(decoded["metadata"]["headers"], Collection):
            return "Original message headers not present or malformed in message data" \
                   f" from {url}"
        for header in decoded["metadata"]["headers"]:
            if not isinstance(header, Collection) or len(header) != 2 \
                    or not isinstance(header[0], str) or not isinstance(header[1], bytes):
                return f"Malformed original message header in message data from {url}"
        if "timestamp" not in decoded["metadata"] \
                or not isinstance(decoded["metadata"]["timestamp"], int):
            return f"Timestamp not present or malformed in message data from {url}"

        return None

    def mark_done(self, metadata, asynchronous: bool = True):
        """Mark a message as fully-processed.

        Args:
            metadata: A Metadata instance containing broker-specific metadata.
            asynchronous: Whether to allow the commit to happen asynchronously
                          in the background.

        """
        self._consumer.mark_done(metadata._raw, asynchronous)

    def stop(self):
        """Stops the runloop of the consumer. Useful when running the
        consumer in a different thread.
        """
        self._consumer.stop()

    def close(self):
        """End all subscriptions and shut down.

        """
        logger.info("closing connection")
        self._consumer.close()

    @staticmethod
    def is_test(message):
        """True if message is a test message (contains '_test' as a header key).

        Args:
            message: The message to test.
        """
        h = message.headers()
        if h is None:
            return False
        else:
            return bool([v for k, v in h if k == "_test"])

    def __iter__(self):
        yield from self.read()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Producer:
    @dataclasses.dataclass
    class TopicRecord:
        last_check_time: float
        max_message_size: int
        producer: adc_producer.Producer

    @dataclasses.dataclass
    class ProducerRecord:
        producer: adc_producer.Producer
        n_users: int

    """
    An event stream opened for writing to a topic.
    Instances of this class should be obtained from :meth:`Stream.open`.
    """

    def __init__(self, broker_addresses, topics, auth, automatic_offload: bool = True,
                 topic_check_period: timedelta = timedelta(seconds=1800), **kwargs):
        """
        Args:
            broker_addresses: The list of bootstrap Kafka broker URLs.
            topic: The name of the topic to which to write.
            auth: An adc.auth.SASLAuth object specifying client authentication
                to use.
            error_callback: A callback which will be called with any
                confluent_kafka.KafkaError objects produced representing internal
                Kafka errors.
            produce_timeout: A datetime.timedelta object specifying the maximum
                time to wait for a message to be sent to Kafka. If zero, sending
                will never time out.
            automatic_offload: If true, when a message is too large for the target
                               topic, and if the broker declares a suitable endpoint,
                               offload the message to the offload service, and send
                               a place-holder 'external' message on the Kafka topic
                               in its place.
            topic_check_period: Period to wait before checking whether the settings
                of a target topic have changed.

        :meta private:
        """
        self.topics = {}
        self.producers = {}
        self.broker_addresses = broker_addresses
        self.auth = auth
        self.automatic_offload = automatic_offload
        self.topic_check_period = topic_check_period

        self.producer_args = _filter_valid_args(adc_producer.ProducerConfig, kwargs)
        self.producer_args["auth"] = self.auth
        if isinstance(broker_addresses, str):
            broker_addresses = [broker_addresses]
        self.producer_args["broker_urls"] = self.broker_addresses
        self.produce_timeout = self.producer_args.get("produce_timeout",
                                                      adc_producer.ProducerConfig.produce_timeout)

        if len(topics) == 1:
            self.default_topic = topics[0]
        else:
            self.default_topic = None

        if len(topics) != 0:
            logger.info(f"connecting to kafka://{','.join(broker_addresses)}")
            settings = self._check_topic_settings(topics)
            for topic, topic_settings in settings.items():
                self._set_producer_for_topic(topic, topic_settings)
            logger.info(f"publishing to topic(s): {topics}")

    def _check_topic_settings(self, topics):
        aconfig = adc_producer.ProducerConfig(broker_urls=self.broker_addresses,
                                              auth=self.auth,
                                              topic=None,
                                              )
        # A large value for when we don't want to try to act on this information
        # 1000000000 is the maximum allowed by librdkafka as of version 2.8.0
        dummy_max = ConfigEntry("max.message.bytes", 1000000000)
        if not self.automatic_offload:
            # This information won't be used anyway, so avoid attempting to check the broker,
            # which may fail, and just return dummy data.
            return {topic: {"max.message.bytes": dummy_max} for topic in topics}

        aclient = AdminClient(aconfig._to_confluent_kafka())
        logger.debug(f"Fetching settings for topics: {topics}")
        query = [ConfigResource(restype=ResourceType.TOPIC, name=topic) for topic in topics]
        futures = aclient.describe_configs(query)
        results = {}
        for resource, future in futures.items():
            try:
                results[resource.name] = future.result()
            except confluent_kafka.KafkaException as ke:
                kerr = ke.args[0]
                # it is a potentially common issue that a user has WRITE permission but not
                # DESCRIBE_CONFIGS permission, in which case we need to keep working in a degraded
                # state, without trying to treat the maximum message size
                if kerr.code() == confluent_kafka.KafkaError.TOPIC_AUTHORIZATION_FAILED:
                    warnings.warn(f"Authorization to describe configs of topic {resource.name} "
                                  "failed; unable to determine maximum allowed message size.")
                    results[resource.name] = {"max.message.bytes": dummy_max}
                else:  # For other problems, let the exception propagate
                    raise
        return results

    def _release_producer_for_topic(self, topic):
        t_record = self.topics[topic]
        p_record = self.producers[t_record.max_message_size]
        p_record.n_users -= 1
        if p_record.n_users == 0:
            p_record.producer.flush()
            p_record.producer.close()
            del self.producers[t_record.max_message_size]

    def _set_producer_for_topic(self, topic, settings):
        max_size = int(settings["max.message.bytes"].value)
        logger.debug(f"Maximum message size for topic {topic} is {max_size} bytes")
        # check whether we already have some producer configured for this size,
        # and if so update its number of uses, otherwise construct a new producer with one user
        if max_size in self.producers:
            logger.debug(" Have an existing producer for this message size limit")
            p_record = self.producers[max_size]
            p_record.n_users += 1
        else:
            logger.debug(" Creating new producer for this message size limit")
            producer = adc_producer.Producer(adc_producer.ProducerConfig(
                message_max_bytes=max_size,
                topic=None,  # this will be explicitly managed when calling write()
                **self.producer_args
            ))
            p_record = Producer.ProducerRecord(producer=producer, n_users=1)
            self.producers[max_size] = p_record
        t_record = Producer.TopicRecord(last_check_time=time.time(),
                                        max_message_size=max_size, producer=p_record.producer)
        self.topics[topic] = t_record
        return t_record

    def _record_for_topic(self, topic):
        if topic in self.topics:
            now = time.time()
            if self.topics[topic].last_check_time + self.topic_check_period.total_seconds() < now:
                settings = self._check_topic_settings([topic])
                if settings[topic]["max.message.bytes"].value \
                        != self.topics[topic].max_message_size:
                    logger.debug(f"Maximum message size for topic {topic} has changed from "
                                 f"{self.topics[topic].max_message_size} to "
                                 f"{settings[topic]['max.message.bytes'].value} bytes")
                    self._release_producer_for_topic(topic)
                    return self._set_producer_for_topic(topic, settings[topic])
            return self.topics[topic]
        else:
            settings = self._check_topic_settings([topic])
            return self._set_producer_for_topic(topic, settings[topic])

    @staticmethod
    def _estimate_message_size(packed_message, headers, key=None):
        """Estimate how many bytes the message will occupy including its
        headers for purpose of comparison to the target topic's configured
        maximum message size.

        Args:
            packed_message: The message to write, which must already be correctly encoded by
                            :meth:`Producer.pack <hop.io.Producer.pack>`
            headers: Any headers to attach to the message, either as a dictionary
                mapping strings to strings, or as a list of 2-tuples of strings.
            key: The optional message key

        Return: The estimated message size, not accounting for any reduction due
                to compression.
        """
        # This is the format/protocol overhead, measured for Kafka 2.8 and 3.3.1
        overhead = 70
        # This is the overhead per header entry
        header_overhead = 2
        size = overhead + len(packed_message)

        def ensure_bytes_like(thing):
            """Force an object which may be string-like to be bytes-like"""
            try:  # check whether thing is bytes-like
                memoryview(thing)
                return thing  # keep as-is
            except TypeError:
                return thing.encode("utf-8")

        if key is not None:
            size += len(ensure_bytes_like(key))
        if isinstance(headers, Mapping):
            for header in headers.items():
                size += len(ensure_bytes_like(header[0])) + \
                    len(ensure_bytes_like(header[1])) + header_overhead
        else:
            for header in headers:
                size += len(ensure_bytes_like(header[0])) + \
                    len(ensure_bytes_like(header[1])) + header_overhead
        return size

    def write(self, message, headers=None,
              delivery_callback=errors.raise_delivery_errors, test=False, topic=None, key=None):
        """Write messages to a stream.


        Args:
            message: The message to write.
            headers: The set of headers requested to be sent with the message, either as a
                     mapping, or as a list of 2-tuples. In either the mapping or the list case,
                     all header keys must be strings and and values should be either string-like or
                     bytes-like objects.
            delivery_callback: A callback which will be called when each message
                is either delivered or permanently fails to be delivered.
            test: Message should be marked as a test message by adding a header
                  with key '_test'.
            topic: The topic to which the message should be sent. This need not be specified if
                   the stream was opened with a URL containing exactly one topic name.
            key: If specified, the Kafka message key
        """
        packed_message, full_headers = self._pack(message, headers, test=test)
        self.write_raw(packed_message, full_headers, delivery_callback, topic, key)

    def write_raw(self, packed_message, headers=None,
                  delivery_callback=errors.raise_delivery_errors, topic=None, key=None):
        """Write a pre-encoded message to the stream.

        This is an advanced interface; for most purposes it is preferable to use
        :meth:`Producer.write <hop.io.Producer.write>` instead.

        Args:
            packed_message: The message to write, which must already be correctly encoded by
                            :meth:`Producer.pack <hop.io.Producer.pack>`
            headers: Any headers to attach to the message, either as a dictionary
                mapping strings to strings, or as a list of 2-tuples of strings.
            delivery_callback: A callback which will be called when each message
                is either delivered or permanently fails to be delivered.
            topic: The topic to which the message should be sent. This need not be specified if
                   the stream was opened with a URL containing exactly one topic name.
            key: If specified, the Kafka message key
        """
        if topic is None and self.default_topic is not None:
            topic = self.default_topic
        if topic is None:
            raise Exception("No topic specified for write: "
                            "Either configure a topic when opening the Stream, "
                            "or specify the topic argument to write()")
        if delivery_callback is None:
            delivery_callback = lambda *args: None  # noqa: E731

        estimated_size = self._estimate_message_size(packed_message, headers, key=key)
        t_record = self._record_for_topic(topic)
        if estimated_size > t_record.max_message_size:
            if not hasattr(self, "offload_url"):
                if self.automatic_offload:
                    self.offload_url = _get_offload_server(self.broker_addresses, self.auth)
                else:
                    self.offload_url = None
            if self.offload_url is not None:
                logger.debug(f"Message is too large (est. {estimated_size} bytes) to fit on the "
                             f"topic; offloading to {self.offload_url}")
                packed_message, headers, err = self._offload_message(packed_message, headers, topic,
                                                                     key)
                if err is not None:
                    delivery_callback(err, Consumer.ExternalMessage.make_error(err))
                    return
                # p_size = self._estimate_message_size(packed_message, headers)
                # Possible edge case: if p_size is _still_ greater than the message size limit,
                # we will have a problem sending the placeholder message
            else:
                err = confluent_kafka.KafkaError(confluent_kafka.KafkaError.MSG_SIZE_TOO_LARGE,
                                                 f"Unable to send message which is {estimated_size}"
                                                 " bytes with headers to topic with message size"
                                                 f" limit of {t_record.max_message_size}",
                                                 False, False, False)
                delivery_callback(err, Consumer.ExternalMessage.make_error(err))
                return
        t_record.producer.write(packed_message, headers=headers,
                                delivery_callback=delivery_callback, topic=topic, key=key)

    def _offload_message(self, message, headers, topic, key=None):
        """Send a large message to an offload API server via HTTP, and generate a replacement
        reference message to be sent to Kafka in its place.

        Args:
            message: The message to be sent which must already be correctly encoded by
                     :meth:`Producer.pack <hop.io.Producer.pack>`
            headers: Any headers to attach to the message as a list of (str, bytes) 2-tuples.
            topic: The name of the topic to which the message is intended to be sent
            key: If specified, the Kafka message key
        Return: A tuple consisting of the replacement message payload, replacement message headers,
                and an error object. In the case of success the first two tuple entries are valid
                and suitable for passing to the Kafka producer's write, and the third tuple entry
                is None. In the case of failure, the first two entries are None, and the error is a
                confluent_kafka.KafkaError object.
        """
        offload_suffix = "+oversized"
        write_url = f"{self.offload_url}/topic/{topic}{offload_suffix}"
        msg_id = None
        test = False
        for header in headers:
            if header[0] == "_test" and header[1] == b"true":
                test = True
            if header[0] == "_id":
                try:
                    msg_id = str(uuid.UUID(bytes=header[1]))
                except ValueError:
                    pass
        if msg_id is None:
            err = confluent_kafka.KafkaError(confluent_kafka.KafkaError._BAD_MSG,
                                             "Message has no ID ('_id' header). "
                                             "Was it properly processed by Producer.pack?",
                                             False, False, False)
            return (None, None, err)
        data_raw = {"message": message, "headers": headers}
        if key is not None:
            data_raw["key"] = key
        data = bson.dumps(data_raw)
        try:
            # We assume that no server will allow un-authenticated writes, so we use shortcut=True
            # to start attempting a SCRAM handshake as quickly as possible.
            resp = requests.post(write_url, data=data,
                                 auth=http_scram.SCRAMAuth(self.auth, shortcut=True))
        except RuntimeError as ex:
            err = confluent_kafka.KafkaError(confluent_kafka.KafkaError.SASL_AUTHENTICATION_FAILED,
                                             "Failed to send large message to offload server at "
                                             f"{write_url}: {str(ex)}", False, False, False)
            return (None, None, err)
        if not resp.ok:
            err = _http_error_to_kafka(resp.status_code,
                                       "Failed to send large message to offload server at "
                                       f"{write_url}: POST request failed with status "
                                       f"{resp.status_code}: {resp.content}")
            return (None, None, err)
        msg_url = f"{self.offload_url}/msg/{msg_id}"
        placeholder = models.ExternalMessage(url=msg_url)
        # It is important that the placeholder be assigned its own ID, so it does not collide in the
        # archive with the 'real' message to which it refers.
        # The only header we copy to the placeholder is the test header, as it makes sense to keep
        # that synchronized, enabling subscribers to skip fetching large test messages, but
        # otherwise subscribers will obtain all headers from fetching the original message, so they
        # do not need to be on the placeholder.
        return self.pack(placeholder, test=test, auth=self.auth) + (None,)

    @staticmethod
    def pack(message, headers=None, test=False, auth=None):
        """Pack and serialize a message.

        This is an advanced interface, which most users should not need to call directly, as
        :meth:`Producer.write <hop.io.Producer.write>` uses it automatically.

        Args:
            message: The message to pack and serialize.
            headers: The set of headers requested to be sent with the message, either as a
                     mapping, or as a list of 2-tuples. In either the mapping or the list case,
                     all header keys must be strings and and values should be either string-like or
                     bytes-like objects.
            test: Message should be marked as a test message by adding a header
                  with key '_test'.

        Returns:
            A tuple containing the serialized message and the collection of headers which
            should be sent with it.

        """
        # canonicalize headers to list form
        if headers is None:
            headers = []
        elif isinstance(headers, Mapping):
            headers = list(headers.items())
        # Assign a UUID to the message
        headers.append(("_id", uuid.uuid4().bytes))
        if auth is not None:
            headers.append(("_sender", auth.username.encode("utf-8")))
        if test:
            headers.append(('_test', b"true"))
        try:  # first try telling the message to serialize itself
            encoded = message.serialize()
            headers.append(("_format", encoded["format"].encode("utf-8")))
            payload = encoded["content"]
        except AttributeError:  # message is not a model object which can self-serialize
            try:  # serialize the message as JSON if possible
                blob = models.JSONBlob(content=message)
                encoded = blob.serialize()
                headers.append(("_format", encoded["format"].encode("utf-8")))
                payload = encoded["content"]
            except TypeError:  # message can't be turned into JSON
                if isinstance(message, bytes):
                    blob = models.Blob(content=message)
                    encoded = blob.serialize()
                    headers.append(("_format", encoded["format"].encode("utf-8")))
                    payload = encoded["content"]
                else:
                    raise TypeError("Unable to pack a message of type "
                                    + message.__class__.__name__
                                    + " which is not bytes and cannot be serialized to JSON")
        return (payload, headers)

    def _pack(self, message, headers, test):
        """Internal wrapper for :meth:`pack <hop.io.Producer.pack>` which
        automatically sets the auth parameter.
        """
        return self.pack(message, headers=headers, test=test, auth=self.auth)

    def flush(self, timeout: Optional[timedelta] = None):
        """Request that any messages locally queued for sending be sent immediately.

        Args:
            timeout: The length of time to wait for messages to send.
                     Defaults to the produce_timeout.
        Returns:
            The number of messages still queued locally to be sent.
        """
        if timeout is None:
            timeout = self.produce_timeout
        still_queued = 0
        for p_record in self.producers.values():
            still_queued += p_record.producer.flush(timeout)
        return still_queued

    def close(self):
        """Wait for enqueued messages to be written and shut down.

        """
        if sum([r.producer.queued_message_count() for r in self.producers.values()]) > 0:
            logger.info("closing connection after queued messages send")
        else:
            logger.info("closing connection")
        still_queued = 0
        for p_record in self.producers.values():
            still_queued += p_record.producer.close(self.produce_timeout)
        # if the user requested never timing out, keep trying as long as there are still unsent
        # messages
        while still_queued > 0 and self.produce_timeout.total_seconds() == 0:
            still_queued = self.flush(timedelta(seconds=1))
        self.producers = {}
        self.topics = {}
        return still_queued

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type == KeyboardInterrupt:
            print("Aborted (CTRL-C).")
            return True
        if type is None and value is None and traceback is None:
            unsent = self.close()
            if unsent > 0:
                raise Exception(f"{unsent} messages remain unsent, some data may have been lost!")
            return False
        return False


def list_topics(url: str, auth: Union[bool, Auth] = True, timeout=-1.0):
    """List the accessible topics on the Kafka broker referred to by url.

    Args:
        url: The Kafka broker URL. Only one broker may be specified. Topics
            may be specified, in which case only topics in the intersection
            of the set specified by the URL and actually present on the broker
            will be returned. If a userinfo component is present in the URL and
            auth is True, it will be treated as a hint to the automatic auth
            lookup.
        auth: A `bool` or :class:`Auth <hop.auth.Auth>` instance. Defaults to
            loading from :meth:`auth.load_auth <hop.auth.load_auth>` if set to
            True. To disable authentication, set to False. If a username is
            specified as part of url but auth is a :class:`Auth <hop.auth.Auth>`
            instance the url information will be ignored.
        timeout: A floating point value, indicating the maximum number of
            seconds to wait to connect to a broker, or a negative value to
            indicate no limit.

    Returns:
        A dictionary mapping topic names to
        :class:`confluent_kafka.admin.TopicMetadata` instances.

    Raises:
        ValueError: If more than one broker is specified.
        confluent_kafka.KafkaException: If connecting to the broker times out.
    """
    username, broker_addresses, query_topics = kafka.parse_kafka_url(url)
    if len(broker_addresses) > 1:
        raise ValueError("Multiple broker addresses are not supported")
    user_auth = None
    if auth is True:
        credentials = load_auth()
        user_auth = select_matching_auth(credentials, broker_addresses[0], username)
    elif auth is not False:
        user_auth = auth
    group_id = _generate_group_id(username, 10)
    config = {
        "bootstrap.servers": ",".join(broker_addresses),
        "error_cb": errors.log_client_errors,
        "group.id": group_id,
    }
    if user_auth is not None:
        config.update(user_auth())
    consumer = confluent_kafka.Consumer(config)
    valid_topics = {}
    if query_topics is not None:
        for topic in query_topics:
            topic_data = consumer.list_topics(topic=topic, timeout=timeout).topics
            for topic in topic_data.keys():
                if topic_data[topic].error is None:
                    valid_topics[topic] = topic_data[topic]
    else:
        topic_data = consumer.list_topics(timeout=timeout).topics
        valid_topics = {t: d for t, d in topic_data.items() if d.error is None}
    return valid_topics
