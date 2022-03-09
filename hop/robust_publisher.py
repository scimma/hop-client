#!/usr/bin/env python

import heapq
from io import BytesIO
import logging
import os
import struct
import threading
import zlib

from . import io

import confluent_kafka
from adc.errors import KafkaException


logger = logging.getLogger("hop")


class _RAPriorityQueue:
    """A priority queue which also allows random access to queued items.

    Items' keys are also their priorities, and keys which compare lower have higher priority.

    All keys in a queue must be mutually comparable; this is most easily accomplished by using a
    single key type for a given queue.

    """

    def __init__(self):
        """Create an empty queue"""
        self.priorities = []
        self.data = {}

    def __len__(self):
        """Return: the number of items in the queue."""
        assert len(self.data) == len(self.priorities)
        return len(self.data)

    def __contains__(self, key):
        """Test whether an item with the given key/priority is present in the queue."""
        return key in self.data

    def insert(self, key, value):
        """Add an item to the queue, or replace the existing item stored under the given key."""
        self.data[key] = value
        heapq.heappush(self.priorities, key)

    def __setitem__(self, key, value):
        """Add an item to the queue, or replace the existing item stored under the given key."""
        self.insert(key, value)

    def __getitem__(self, key):
        """Fetch the item stored under the given key.

        Raises: KeyError if the key does not exist.
        """
        return self.data[key]

    def pop_highest_priority(self):
        """Fetch the item with the highest priority (lowest key) currently in the queue,
            and removes it from the queue.

        Returns: The (key, value) tuple for the highest priority item in the queue, or None if the
                 queue is empty.
        """
        if len(self.data) == 0:
            return None
        key = heapq.heappop(self.priorities)
        value = self.data[key]
        del self.data[key]
        return (key, value)

    def remove(self, key):
        """Remove an item from the queue, regardless of its location.

        Raises: KeyError if the key does not exist.
        """
        del self.data[key]
        idx = self.priorities.index(key)
        del self.priorities[idx]
        heapq.heapify(self.priorities)

    def __delitem__(self, key):
        self.remove(key)


def _ensure_bytes_like(thing):
    """Force an object which may be string-like to be bytes-like

    Args:
        thing: something which might be a string or might already be encoded as bytes.

    Return:
        Either the original input object or the encoding of that object as bytes.
    """
    try:  # check whether thing is bytes-like
        memoryview(thing)
        return thing  # keep as-is
    except TypeError:
        return thing.encode("utf-8")


class PublicationJournal:
    """
    An object which tracks the state of messages which are being sent, persists that state to disk,
    and enables it to be restored if the program stops unexpectedly.
    """

    int_format = "!Q"
    int_size = struct.calcsize(int_format)
    crc_format = "!I"
    crc_size = struct.calcsize(crc_format)
    msg_record_type = 0
    sent_record_type = 1

    @staticmethod
    def encode_int(value: int):
        return(struct.pack(PublicationJournal.int_format, value))

    @staticmethod
    def decode_int(buffer: bytes):
        return struct.unpack(PublicationJournal.int_format, buffer)[0]

    @staticmethod
    def encode_crc(crc: int):
        return(struct.pack(PublicationJournal.crc_format, crc))

    @staticmethod
    def decode_crc(buffer: bytes):
        return struct.unpack(PublicationJournal.crc_format, buffer)[0]

    def __init__(self, journal_path="publisher.journal"):
        """Prepare a journal, including loading any data previously persisted to disk.

        Args:
            journal_path: The filesystem path from/to which the journal data should be read/written.

        Raises:
            PermissionError: If existing journal file does not have suitable permissions.
            RuntimeError: If existing journal data cannot be read.
        """
        self.journal_path = journal_path
        # each queue will store (message, headers) tuples, keyed by sequence numbers
        self.messages_to_send = _RAPriorityQueue()
        self.maybe_sent_messages = _RAPriorityQueue()
        self.message_counter = 0
        self._read_previous_journal()
        self.journal = open(self.journal_path, "ab")

    def _write_record(self, record_type: int, record_body: bytes):
        try:
            body_crc = zlib.crc32(record_body, 0) & 0xFFFFFFFF
            header_crc = 0

            def write_to_header(data: bytes):
                # update running checksum and write data
                nonlocal header_crc
                header_crc = zlib.crc32(data, header_crc) & 0xFFFFFFFF
                self.journal.write(data)
            # The header is a fixed length, so it can be read independent of its contents.
            # The body length is contained in the header, and protected by the header CRC
            # so that item lengths within the body can be sanity checked before reading the
            # body is complete. The body CRC also protects the data in the body, but cannot
            # be re-checked until the entire body has been read. That can pose a problem if
            # the length for one of the variable length components in the body is corrupted,
            # and would lead to read of a nonsensical length (exhausting memory or similar).
            write_to_header(PublicationJournal.encode_int(record_type))  # record type
            write_to_header(PublicationJournal.encode_int(len(record_body)))  # body length
            write_to_header(PublicationJournal.encode_crc(body_crc))  # body CRC
            self.journal.write(PublicationJournal.encode_crc(header_crc))  # header CRC
            self.journal.write(record_body)  # body
            self.journal.flush()
        except BaseException as e:
            raise RuntimeError(f"Failed to append record to journal: {e}")

    # returns the sequence number assigned to the message
    def queue_message(self, message: bytes, headers=None):
        """Record to the journal a message which is to be sent.

        Args:
            message: A message to send, encoded as a bytes-like object.
            headers: Headers to be sent with the message, as a list of 2-tuples of bytes-like
                     objects.

        Returns:
            The sequence number assigned to the message.
            Sequence numbers are unique among all messages which are 'live' at the same time,
            but will otherwise be recycled.

        Raises:
            RuntimeError: If appending the new message to the on-disk journal fails.
            TypeError: If the message is not a suitable type (bytes)
        """
        message = _ensure_bytes_like(message)
        assert self.message_counter not in self.messages_to_send
        sequence_number = self.message_counter

        rbody = BytesIO()
        rbody.write(PublicationJournal.encode_int(sequence_number))
        rbody.write(PublicationJournal.encode_int(len(message)))  # message size
        rbody.write(message)  # message data
        if headers is not None:
            rbody.write(PublicationJournal.encode_int(len(headers)))  # number of headers
            for header in headers:
                # data must be encoded to be written
                key = _ensure_bytes_like(header[0])
                value = _ensure_bytes_like(header[1])
                rbody.write(PublicationJournal.encode_int(len(key)))  # header key length
                rbody.write(key)  # header key
                rbody.write(PublicationJournal.encode_int(len(value)))  # header value length
                rbody.write(value)  # header value
        else:
            rbody.write(PublicationJournal.encode_int(0))  # no headers
        self._write_record(PublicationJournal.msg_record_type, rbody.getvalue())

        self.messages_to_send[sequence_number] = (message, headers)
        self.message_counter += 1
        return sequence_number

    def has_messages_to_send(self):
        """Check whether there are messages queued for sending (which have either not been sent at
            all, or for which all sending attempts so far have failed, causing them to be requeued).
        """
        return len(self.messages_to_send) > 0

    def get_next_message_to_send(self):
        """Fetch the next message which should be sent

        Returns:
            The next message in the form of a tuple of (seqeunce number, message, message headers),
            or None if there are no messages currently needing to be sent.
        """
        if len(self.messages_to_send) == 0:
            return None
        result = self.messages_to_send.pop_highest_priority()
        self.maybe_sent_messages[result[0]] = result[1]
        # rearrange to be more friendly to downstream code
        return (result[0], result[1][0], result[1][1])

    def has_messages_in_flight(self):
        """Check whether there are messages for which a sending attempt has been started,
            but has not yet conclusively succeeded or failed
        """
        return len(self.maybe_sent_messages) > 0

    def mark_message_sent(self, sequence_number):
        """Mark a message as successfully sent, and removes it from further consideration.

        Truncates and restarts the backing journal file if the number of messages in-flight and
        waiting to be sent falls to zero, and restarts the sequence number assignment sequence.

        Raises:
            RuntimeError: If no message with the specifed sequence number is currently recorded as
                being in-flight.
        """
        if sequence_number not in self.maybe_sent_messages:
            raise RuntimeError("No record of message with sequence number " + str(sequence_number)
                               + " being queued for sending")
        self._write_record(PublicationJournal.sent_record_type,
                           PublicationJournal.encode_int(sequence_number))
        del self.maybe_sent_messages[sequence_number]
        if len(self.messages_to_send) == 0 and len(self.maybe_sent_messages) == 0:
            # take this opportunity to garbage collect the journal file
            self.journal.close()
            os.unlink(self.journal_path)
            self.journal = open(self.journal_path, "wb")
            self.message_counter = 0

    def requeue_message(self, sequence_number):
        """Record a message send attempt as having failed by moving the message back from the
            in-flight pool to the queue of messages needing to be sent.

        Raises:
            RuntimeError: If no message with the specifed sequence number is currently recorded as
                being in-flight.
        """
        if sequence_number not in self.maybe_sent_messages:
            raise RuntimeError("No record of message with sequence number "
                               f"{sequence_number} being queued for sending")
        # nothing to record in journal, message was already written when first queued
        message = self.maybe_sent_messages[sequence_number]
        del self.maybe_sent_messages[sequence_number]
        self.messages_to_send[sequence_number] = message

    class _ReadPosition:
        """A type for tracking logical read positions in a journal file."""

        def __init__(self):
            self.record_index = 0
            self.record_start = 0

    @staticmethod
    def _read_raw_from_journal(journal, pos_info, size, name, allow_eof=False):
        """Read a specified size of raw data from a strream, producing an error if it cannot be fully
            read and end of file is not indicated to be tolerable.

        Args:
            journal: the stream from which to read
            pos_info: logical position information used for error messages
            size: the amount of data to read
            name: description of the data to be read, for error messages
            allow_eof: whether end of file is tolerable

        Returns:
            The block of data which was read

        Raises:
            RuntimeError: If the specified amount of data was not successfully read.
        """
        try:
            buffer = journal.read(size)
        except Exception:
            raise RuntimeError(f"Journal corrupted: Unable to read {name}")
        if len(buffer) == 0 and allow_eof:  # end of file
            return buffer, True
        elif len(buffer) < size:
            raise RuntimeError("Journal corrupted: Unexpected end of file (unable to read "
                               f"{name}) during record {pos_info.record_index} which began at file "
                               f"offset {pos_info.record_start}")
        if allow_eof:
            return buffer, False
        return buffer

    @staticmethod
    def _decode_raw_data(data, decoder, crc, name):
        """Decode a block of raw data and update a running CRC.

        Args:
            data: the raw data to decode
            decoder: the specific decoding function to apply
            crc: the old value of the running CRC
            name: a description of the data for error messages

        Returns:
            A tuple of the result of applying the decoder and the updated CRC

        Raises:
            RuntimeError: If the decoder produces an error
        """
        crc = zlib.crc32(data, crc) & 0xFFFFFFFF
        try:
            decoded = decoder(data)
            return decoded, crc
        except BaseException as e:
            raise RuntimeError(f"Failed to decode {name}: {e}")

    @staticmethod
    def _read_recorded_header(journal, pos_info, bcrc, required_body_len, rec_len):
        """Extract a recorrded message header from a stream.

        Args:
            journal: the stream from which to read
            pos_info: logical position information used for error messages
            bcrc: the CRC of the message body as read so far
            required_body_len: the length the message record body must be to accomodate all
                               claimed data
            rec_len: the length of the message record body claimed in the record header

        Returns:
            A tuple consisting of the header key, header value, updated record body CRC, and
            updated required record body length.

        Raises:
            RuntimeError: If any read operation fails, data item cannot be decoded, or the supposed
                          header data does not fit inside the claimed record body length.
        """
        decode_int = PublicationJournal.decode_int
        int_size = PublicationJournal.int_size
        read = PublicationJournal._read_raw_from_journal
        decode = PublicationJournal._decode_raw_data

        raw_key_len = read(journal, pos_info, int_size, "message header key length")
        key_len, bcrc = decode(raw_key_len, decode_int, bcrc,
                               "message header key length")
        if key_len > (rec_len - required_body_len):
            raise RuntimeError(f"Journal corrupted: Claimed message header key length "
                               f"({key_len}) exceeds remaining space in record body for"
                               f" record {pos_info.record_index} which began at file offset "
                               f"{pos_info.record_start}")
        required_body_len += key_len

        key = read(journal, pos_info, key_len, "message header key data")
        bcrc = zlib.crc32(key, bcrc) & 0xFFFFFFFF

        raw_val_len = read(journal, pos_info, int_size, "message header value length")
        val_len, bcrc = decode(raw_val_len, decode_int, bcrc,
                               "message header value length")
        if val_len > (rec_len - required_body_len):
            raise RuntimeError(f"Journal corrupted: Claimed message header value length"
                               f" ({val_len}) exceeds remaining space in record body "
                               f"for record {pos_info.record_index} which began at file offset "
                               f"{pos_info.record_start}")
        required_body_len += val_len

        val = read(journal, pos_info, val_len, "message header value data")
        bcrc = zlib.crc32(val, bcrc) & 0xFFFFFFFF

        # confluent kafka requires keys in the form of unicode strings
        try:
            key = key.decode("utf-8")
        except UnicodeError:
            raise RuntimeError(f"Journal corrupted: Message header key is not valid "
                               f"UTF-8 in record {pos_info.record_index} which began at file "
                               f"offset {pos_info.record_start}")
        return (key, val, bcrc, required_body_len)

    def _read_previous_journal(self):
        """Reload journal data from a previous session from disk.

        Raises:
            PermissionError: If the journal file does not have suitable permissions.
            RuntimeError: If a problem occurs reading or interpreting the stored journal data.
        """
        if not os.path.exists(self.journal_path):
            return  # nothing to do!
        journal = open(self.journal_path, "rb")
        rpos = PublicationJournal._ReadPosition()
        read = PublicationJournal._read_raw_from_journal
        decode = PublicationJournal._decode_raw_data
        read_header = PublicationJournal._read_recorded_header
        while True:
            decode_int = PublicationJournal.decode_int
            decode_crc = PublicationJournal.decode_crc

            rpos.record_start = journal.tell()

            # Read the record header:
            hcrc = 0  # header CRC
            record_type_offset = journal.tell()
            raw_rec_type, eof = read(journal, rpos, self.int_size, "record type", allow_eof=True)
            if eof:
                break
            rec_type, hcrc = decode(raw_rec_type, decode_int, hcrc, "record type")

            raw_rec_len = read(journal, rpos, self.int_size, "record body length")
            rec_len, hcrc = decode(raw_rec_len, decode_int, hcrc, "record body length")

            raw_orig_body_crc = read(journal, rpos, self.crc_size, "record body CRC")
            orig_body_crc, hcrc = decode(raw_orig_body_crc, decode_crc, hcrc, "record body CRC")

            raw_orig_hdr_crc = read(journal, rpos, self.crc_size, "record header CRC")
            # don't update recalculated CRC with raw_orig_hdr_crc, since it did not include itself
            orig_hdr_crc, _ = decode(raw_orig_hdr_crc, decode_crc, 0, "record header CRC")

            if hcrc != orig_hdr_crc:
                raise RuntimeError(f"Journal corrupted: Header CRC mismatch (original: "
                                   f"{orig_hdr_crc:x}, recalculated: {hcrc:x}) for record "
                                   f"{rpos.record_index} which began at file offset "
                                   f"{rpos.record_start}")

            # Read the record body:
            bcrc = 0  # body CRC

            if rec_type == PublicationJournal.msg_record_type:
                # record must contain sequence number, message data len, number of mesasge headers
                required_body_len = 3 * self.int_size
                if rec_len < required_body_len:
                    raise RuntimeError(f"Journal corrupted: Claimed record body length ({rec_len}) "
                                       f"too small to conain required data for record "
                                       f"{rpos.record_index} which began at file offset "
                                       f"{rpos.record_start}")

                raw_seq_num = read(journal, rpos, self.int_size, "message sequence number")
                seq_num, bcrc = decode(raw_seq_num, decode_int, bcrc, "sequence number")

                raw_msg_len = read(journal, rpos, self.int_size, "record message length")
                msg_len, bcrc = decode(raw_msg_len, decode_int, bcrc, "message length")
                if msg_len > (rec_len - required_body_len):
                    raise RuntimeError(f"Journal corrupted: Claimed message data length ({msg_len})"
                                       f" exceeds record body length for record "
                                       f"{rpos.record_index} which began at file offset "
                                       f"{rpos.record_start}")
                required_body_len += msg_len

                message_data = read(journal, rpos, msg_len, "record message data")
                bcrc = zlib.crc32(message_data, bcrc) & 0xFFFFFFFF

                raw_msg_hdr_cnt = read(journal, rpos, self.int_size, "record message header count")
                msg_hdr_cnt, bcrc = decode(raw_msg_hdr_cnt, decode_int, bcrc,
                                           "message header count")
                # each header is described by a minimum of two encoded integers (key len and value
                # len), so sanity check that the claimed number of headers would fit in the
                # remaining space in the record
                if 2 * self.int_size * msg_hdr_cnt > (rec_len - required_body_len):
                    raise RuntimeError(f"Journal corrupted: Claimed number of message headers "
                                       f"({msg_hdr_cnt}) exceeds remaining space in record body for"
                                       f" record {rpos.record_index} which began at file offset "
                                       f"{rpos.record_start}")
                required_body_len += 2 * self.int_size * msg_hdr_cnt

                message_headers = []
                for i in range(0, msg_hdr_cnt):
                    key, val, bcrc, required_body_len = \
                        read_header(journal, rpos, bcrc, required_body_len, rec_len)
                    message_headers.append((key, val))

                if seq_num in self.messages_to_send:
                    raise RuntimeError("Journal corrupted: Duplicate message sequence number in "
                                       f"record {rpos.record_index} which began "
                                       f"at file offset {rpos.record_start}")
                self.messages_to_send[seq_num] = (message_data, message_headers)
                if self.message_counter <= seq_num:
                    self.message_counter = seq_num + 1

            elif rec_type == PublicationJournal.sent_record_type:
                raw_seq_num = read(journal, rpos, self.int_size, "message sequence number")
                seq_num, bcrc = decode(raw_seq_num, decode_int, bcrc, "sequence number")

                if seq_num in self.messages_to_send:
                    del self.messages_to_send[seq_num]
                else:
                    raise RuntimeError("Journal corrupted: Record of sent message ("
                                       f"{seq_num} which did not previously appear, in "
                                       f"record {rpos.record_index} which began "
                                       f"at file offset {rpos.record_start}")
            else:
                raise RuntimeError(f"Journal corrupted: Invalid record type ({rec_type}) "
                                   f"at file offset {record_type_offset}")

            if bcrc != orig_body_crc:
                raise RuntimeError(f"Journal corrupted: CRC mismatch (original: {orig_body_crc:x}, "
                                   f"recalculated: {bcrc:x}) for record {rpos.record_index} which "
                                   f"began at file offset {rpos.record_start}")

            rpos.record_index += 1

    class NullLock:
        """A trivial context manager-compatible class which can be used in place of a lock when no
        locking is needed."""

        def __init__(self):
            pass

        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_value, exc_traceback):
            pass

    def _delivery_callback(self,
                           kafka_error: confluent_kafka.KafkaError,
                           msg: confluent_kafka.Message,
                           seq_num: int,
                           lock):
        """Handle a callback from librdkafka indicating the outcome of sending a message by either
            marking it successfully sent or requeuing it to send again.
        """
        if kafka_error is None and msg.error() is None:
            logger.debug(f"Message with sequence number {seq_num} was confirmed sent in "
                         f"{msg.latency()} seconds")
            with lock:
                self.mark_message_sent(seq_num)
        else:
            err = kafka_error
            if err is None:
                err = msg.error()
            logger.error("Error delivering message with sequence number: %i: "
                         "%s; requeuing to send again", seq_num, err)
            with lock:
                self.requeue_message(seq_num)

    def get_delivery_callback(self, seq_num, lock=NullLock()):
        """Construct a callback handler specific to a particular message which will either mark it
        successfully sent or requeue it to send again.

        The callback which is produced will take two arguments: A confluent_kafka.KafkaError
        describing any error in sending the message, and confluent_kafka.Message containing the
        message itself.

        Args:
            seq_num: The sequence number of the message in question, previously returned by
                     :meth:`get_next_message_to_send`.
            lock: An optional reference to a lock object which the callback should hold when
                  invoked, e.g. to protect concurrent access to the journal.

        """
        if seq_num not in self.maybe_sent_messages:
            raise RuntimeError("Cannot produce a delivery callback for message with sequence "
                               f"number {seq_num} which is not in flight")
        return lambda err, msg: self._delivery_callback(err, msg, seq_num, lock)

    # adc.errors.log_client_errors is dangerous as it throws exceptions
    # which will trigger https://github.com/confluentinc/confluent-kafka-python/issues/729
    @staticmethod
    def error_callback(kafka_error: confluent_kafka.KafkaError):
        """A safe callback handler for reporting Kafka errors."""
        logger.error(f"Kafka error: {kafka_error}")


class RobustProducer(threading.Thread):
    def __init__(self, url, auth=True, journal_path="publisher.journal", poll_wait=1.e-4, **kwargs):
        """Construct a publisher which will retry sending messages if it does not receive confirmation
        that they have arrived, including if it is itself taken offline (i.e. crashes) for some
        reason.

        This is intended to provide *at least once* delivery of messages: If a message is confirmed
        received by the broker, it will not be sent again, but if any disruption of the network or
        the publisher itself prevents it from receiving that confirmation, even if the message was
        actually received by the broker, the publisher will assume the worst and send the message
        again. Users of this class (and more generally consumers of data published with it) should
        be prepared to discard duplicate messages.

        Args:
            url: The URL for the Kafka topci to which messages will be published.
            auth: A `bool` or :class:`Auth <hop.auth.Auth>` instance. Defaults to
                  loading from :meth:`auth.load_auth <hop.auth.load_auth>` if set to
                  True. To disable authentication, set to False.
            journal_path: The path on the filesystem where the messages being sent should be
                          recorded until they are known to have been successfully received. This
                          path should be located somewhere that will survive system restarts, and if
                          messages contain sensitive data it should be noted that they will be
                          written unencrypted to this path. The journal size is generally limited to
                          the sum of sizes of messages queued for sending or in flight at the same
                          time, plus some small (few tens of bytes per message) bookkeeping
                          overhead. Note that this size can become large is a lengthy network
                          disruption prev ents messages from being sent; enough disk spacec should
                          be available to cover this possibility for the expected message rate and
                          duration of disruptions which may need to be handled.
            poll_wait: The time the publisher should spend checking for receipt of each message
                       directly after sending it. Tuning this parameter controls a tradeoff between
                       low latency discovery of successful message delivery and throughput. If the
                       time between sending messages is large compared to the latency for a message
                       to be sent and for a confirmation of receipt to return, it is useful to
                       increase this value so that the publisher will wait to discover that each
                       message has been sent (in the success case) instead of sleeping and waiting
                       for another message to send. If this value is 'too low' (much smaller than
                       both the time for a message to be sent and acknowledged and the time for the
                       next message to be ready for sending), the publisher will waste CPU time
                       entering and exiting the internal function used to receive event
                       notifications. If this value is too large (larger than or similar in size to
                       the time between messages needing to be sent) throughput will be lost as time
                       will be spent waiting to see if the previous message has been acknowledged
                       which could be better spent getting the next message sent out. When in doubt,
                       it is probably best to err on the side of choosing a small value.
            kwargs: Any additional arguments to be passed to :meth:`hop.io.open <hop.io.open>`.

        Raises:
            OSError: If a journal file exists but cannot be read.
            Runtime Error: If the contents of the journal file are corrupted.

        """
        super(RobustProducer, self).__init__()

        self._poll_wait = poll_wait

        # read any messages left over from a previous run
        self._journal = PublicationJournal(journal_path)
        if self._journal.has_messages_to_send():
            logger.info(f"Journal has {len(self._journal.messages_to_send)} "
                        "left-over messages to send")

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

        dummy = io.Stream(auth=auth)
        self._stream = dummy.open(url, "w", error_callback=PublicationJournal.error_callback,
                                  **kwargs)

    def run(self):
        """This method is not part of the public interface of this class, and should not be called directly
        by users.

        """
        while True:
            with self._cond:
                if self._should_stop and not self._journal.has_messages_to_send():
                    break
                if not self._immediate_start:
                    self._cond.wait()  # wait for something to do
                else:
                    self._immediate_start = False
            self._do_send()
        self._stream.flush()

    def _do_send(self):
        journal = self._journal
        # use this slightly awkward loop structure to reacquire the lock on each iteration so that
        # other threads have a chance to take it
        while True:
            with self._lock:
                busy = journal.has_messages_to_send() or journal.has_messages_in_flight()
                if not busy:
                    break  # no work to do for now
                do_send = False
                if journal.has_messages_to_send():
                    seq_num, message, headers = journal.get_next_message_to_send()
                    do_send = True
            if do_send:
                try:
                    dc = journal.get_delivery_callback(seq_num, self._lock)
                    logger.debug(f"Sending message with sequence number {seq_num}")
                    self._stream.write_raw(message, headers=headers, delivery_callback=dc)
                except KafkaException as e:
                    logger.error(f"Error sending message with sequence number: {seq_num}: {e}"
                                 "; requeuing to send again")
                    with self._lock:
                        journal.requeue_message(seq_num)
            try:
                self._stream._producer._producer.poll(self._poll_wait)
            except KafkaException:
                pass

    def write(self, message, headers=None):
        """Queue a message to be sent. Message sending occurs asynchronously on a background thread, so
        this method returns immediately unless an error occurs queuing the message.
        :meth:`RobustProducer.start <RobustProducer.start>` must be called prior to calling this
        method.

        Args:
            message: A message to send.
            headers: Headers to be sent with the message, as a list of 2-tuples of strings.

        Raises:
            RuntimeError: If appending the new message to the on-disk journal fails.
            TypeError: If the message is not a suitable type.

        """
        message, headers = io.Producer.pack(message, headers)
        with self._cond:  # must hold the lock to manipulate journal
            seq_num = self._journal.queue_message(message, headers)
            self._cond.notify()  # wake up the sender loop if sleeping
        logger.debug(f"Queued message with sequence number {seq_num} to be sent")

    def start(self):
        """Start the background communication thread used by the publisher to send messages. This should
        be called prior to any calls to :meth:`RobustProducer.write <RobustProducer.write>`.
        This method should not be called more than once.

        """
        self._should_stop = False
        self._immediate_start = self._journal.has_messages_to_send()
        super(RobustProducer, self).start()

    def stop(self):
        """Stop the background communication thread used by the publisher to send messages. This method
        will block until the thread completes, which includes sending all queued messages.
        :meth:`RobustProducer.write <RobustProducer.write>` should not be called after this method
        has been called.
        This method should not be called more than once.

        """
        logger.debug("Stopping publisher thread")
        with self._cond:
            self._should_stop = True
            self._cond.notify()  # wake up the sender loop
        self.join()
        self._stream.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, ex_type, ex_val, traceback):
        self.stop()
        return False  # propagate any exceptions
