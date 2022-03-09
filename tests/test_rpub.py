import errno
from io import BytesIO
import logging
import os
import pytest
import stat
import struct
import time
import threading
from unittest.mock import patch, MagicMock

from adc.errors import KafkaException

import hop
from hop.robust_publisher import _RAPriorityQueue, PublicationJournal, RobustProducer


logger = logging.getLogger("hop")


def test_queue_construction():
    q = _RAPriorityQueue()
    assert len(q) == 0  # queue should initially be empty


def test_queue_len():
    q = _RAPriorityQueue()
    expected = 0

    def add(key, value):
        nonlocal expected
        q.insert(key, value)
        expected += 1

    def remove():
        nonlocal expected
        result = q.pop_highest_priority()
        if result is not None:
            expected -= 1

    add(1, "a")
    assert len(q) == expected
    add(2, "b")
    assert len(q) == expected
    remove()
    assert len(q) == expected
    add(0, "c")
    assert len(q) == expected
    remove()
    assert len(q) == expected
    remove()
    assert len(q) == expected
    remove()
    assert len(q) == expected


def test_queue_contains():
    q = _RAPriorityQueue()
    assert "foo" not in q
    q.insert("foo", "bar")
    assert "foo" in q
    q.pop_highest_priority()
    assert "foo" not in q
    q.insert("baz", "quux")
    q.insert("xen", "hom")
    assert "baz" in q
    assert "xen" in q


def test_queue_insert_setitem():
    q = _RAPriorityQueue()
    assert "foo" not in q
    q.insert("foo", "bar")
    assert "foo" in q
    assert q["foo"] == "bar"
    q.insert("foo", "baz")
    assert q["foo"] == "baz"
    q["foo"] = "quux"
    assert q["foo"] == "quux"
    q["bar"] = "xen"
    assert q["bar"] == "xen"


def test_queue_getitem():
    q = _RAPriorityQueue()
    with pytest.raises(KeyError):
        q["foo"]
    q.insert("foo", "bar")
    assert q["foo"] == "bar"
    q.insert("baz", "quux")
    assert q["foo"] == "bar"
    assert q["baz"] == "quux"
    q.pop_highest_priority()
    assert q["foo"] == "bar"
    with pytest.raises(KeyError):
        q["baz"]


def test_queue_pop():
    q = _RAPriorityQueue()
    assert q.pop_highest_priority() is None
    # if there is only one item, pop must remove it
    q.insert(1, "foo")
    assert q.pop_highest_priority() == (1, "foo")
    # multiple items must be removed in priority order
    q.insert(2, "bar")
    q.insert(3, "baz")
    q.insert(1, "foo")
    assert q.pop_highest_priority() == (1, "foo")
    assert q.pop_highest_priority() == (2, "bar")
    assert q.pop_highest_priority() == (3, "baz")


def test_queue_remove_del():
    q = _RAPriorityQueue()
    with pytest.raises(KeyError):
        q.remove(1)
    q.insert(2, "bar")
    q.insert(3, "baz")
    q.insert(1, "foo")
    q.remove(2)
    assert 2 not in q
    assert q.pop_highest_priority() == (1, "foo")
    assert q.pop_highest_priority() == (3, "baz")
    with pytest.raises(KeyError):
        del q[1]
    q.insert(2, "bar")
    q.insert(3, "baz")
    q.insert(1, "foo")
    del q[1]
    assert 1 not in q
    assert q.pop_highest_priority() == (2, "bar")
    assert q.pop_highest_priority() == (3, "baz")


########################################


def test_journal_construct_no_file(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)
    assert not j.has_messages_to_send()
    assert not j.has_messages_in_flight()
    assert j.get_next_message_to_send() is None
    assert os.path.exists(journal_path)


def test_journal_construct_empty_file(tmpdir):
    journal_path = tmpdir.join("journal")
    open(journal_path, "ab")  # touch file
    j = PublicationJournal(journal_path)
    assert not j.has_messages_to_send()
    assert j.get_next_message_to_send() is None
    assert not j.has_messages_in_flight()
    assert os.path.exists(journal_path)


def test_journal_construct_bad_perms_file(tmpdir):
    journal_path = tmpdir.join("journal")
    open(journal_path, "ab")  # touch file
    os.chmod(journal_path, stat.S_IWUSR)  # remove read permission
    with pytest.raises(PermissionError):
        j = PublicationJournal(journal_path)


def test_journal_queue_message(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    j.queue_message(b"message 0")
    assert j.has_messages_to_send()
    assert not j.has_messages_in_flight()
    s0 = j.get_next_message_to_send()
    assert s0[1] == b"message 0"
    assert not j.has_messages_to_send()
    assert j.has_messages_in_flight()

    headers = [(b"header_0", b"value_0"), (b"header_1", b"value_1")]
    j.queue_message(b"message 1", headers=headers)
    assert j.has_messages_to_send()
    s1 = j.get_next_message_to_send()
    assert not j.has_messages_to_send()
    assert s1[1] == b"message 1"
    assert s1[2] == headers


def test_journal_queue_message_write_failure(tmpdir):
    def write_fail(*args, **kwargs):
        raise OSError(errno.EIO, os.strerror(errno.EIO))
    file_mock = MagicMock()
    file_mock.write = write_fail
    file_mock.__enter__ = MagicMock(return_value=file_mock)
    open_mock = MagicMock(return_value=file_mock)

    journal_path = tmpdir.join("journal")
    with patch("builtins.open", open_mock):
        j = PublicationJournal(journal_path)
        with pytest.raises(RuntimeError) as excinfo:
            j.queue_message(b"some data")
        assert "Failed to append record to journal" in str(excinfo.value)


def test_journal_mark_sent(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    j.queue_message(b"message 0")
    assert j.has_messages_to_send()
    assert not j.has_messages_in_flight()
    s0 = j.get_next_message_to_send()
    assert s0[1] == b"message 0"
    assert j.has_messages_in_flight()

    j.mark_message_sent(s0[0])
    assert not j.has_messages_to_send()
    assert not j.has_messages_in_flight()

    with pytest.raises(RuntimeError):
        j.mark_message_sent(s0[0])  # can't mark the same message sent twice

    j.queue_message(b"message 1")
    j.queue_message(b"message 2")
    assert j.has_messages_to_send()

    with pytest.raises(RuntimeError):
        j.mark_message_sent(1)  # can't mark things sent that haven't been extracted

    s1 = j.get_next_message_to_send()
    s2 = j.get_next_message_to_send()
    assert not j.has_messages_to_send()
    assert j.has_messages_in_flight()

    # can mark messages sent out of order
    j.mark_message_sent(s2[0])
    assert j.has_messages_in_flight()
    j.mark_message_sent(s1[0])
    assert not j.has_messages_in_flight()


def test_journal_mark_sent_write_failure(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)
    j.queue_message(b"message 0")
    del j

    def write_fail(*args, **kwargs):
        raise OSError(errno.EIO, os.strerror(errno.EIO))
    file_mock = MagicMock()
    file_mock.write = write_fail
    file_mock.__enter__ = MagicMock(return_value=file_mock)
    orig_open = open
    # allow reading to proceed normally so we can read the existing journal,
    # but intercept writing to inject failures

    def sneaky_open(path, mode, *args, **kwargs):
        if 'w' in mode or 'a' in mode:
            return file_mock
        else:
            return orig_open(path, mode, *args, **kwargs)

    with patch("builtins.open", wraps=sneaky_open):
        j = PublicationJournal(journal_path)
        s = j.get_next_message_to_send()
        with pytest.raises(RuntimeError) as excinfo:
            j.mark_message_sent(s[0])
        assert "Failed to append record to journal" in str(excinfo.value)


def test_journal_requeue_message(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    with pytest.raises(RuntimeError):
        j.requeue_message(0)  # can't requeue unknown messages

    j.queue_message(b"message 0")
    with pytest.raises(RuntimeError):
        j.requeue_message(0)  # can't requeue unsent messages

    s0 = j.get_next_message_to_send()
    assert not j.has_messages_to_send()
    assert j.has_messages_in_flight()
    j.requeue_message(s0[0])
    assert j.has_messages_to_send()
    assert not j.has_messages_in_flight()

    with pytest.raises(RuntimeError):
        j.requeue_message(s0[0])  # can't requeue the same message twice at a time

    s0_2 = j.get_next_message_to_send()
    assert s0_2 == s0, "Message to be resent should match original"
    j.mark_message_sent(s0_2[0])

    with pytest.raises(RuntimeError):
        j.requeue_message(s0[0])  # can't requeue a sent message


def test_journal_ecoder_failure():
    format = "!I"

    def even_number_decoder(buffer):
        if buffer[-1] & 1:
            raise RuntimeError("Data does not represent an even number")
        return struct.unpack(format, buffer)[0]

    good_data = struct.pack(format, 222)
    bad_data = struct.pack(format, 32767)

    PublicationJournal._decode_raw_data(good_data, even_number_decoder, 0, "my value")
    with pytest.raises(RuntimeError) as err:
        PublicationJournal._decode_raw_data(bad_data, even_number_decoder, 0, "my value")
    assert("my value" in str(err.value))
    assert("Data does not represent an even number" in str(err.value))


def test_journal_restore_state(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    del j
    j = PublicationJournal(journal_path)  # reload empty journal
    assert not j.has_messages_to_send()
    assert not j.has_messages_in_flight()

    m0 = b"message 0"
    headers = [("header_0", b"value_0"), ("header_1", b"value_1")]
    j.queue_message(m0, headers)
    del j
    j = PublicationJournal(journal_path)  # reload with message in queue
    assert j.has_messages_to_send()
    assert not j.has_messages_in_flight()

    s0 = j.get_next_message_to_send()
    assert s0[1] == m0, "Message body should be preserved by on-disk journal"
    assert s0[2] == headers, "Message headers should be preserved by on-disk journal"
    del j
    j = PublicationJournal(journal_path)  # reload with message in flight
    # this may appear counter-intuitive, but if the journal/sender is offline while the message is
    # in flight, we have no way of knowing whether it arrived. Therefore, the conservative thing to
    # do is assume that it did not, and send it again.
    assert j.has_messages_to_send()
    assert not j.has_messages_in_flight()

    s0_2 = j.get_next_message_to_send()
    assert s0_2 == s0, "Message to be resent should match original"
    j.mark_message_sent(s0_2[0])
    del j
    j = PublicationJournal(journal_path)  # reload after message successfully sent
    assert not j.has_messages_to_send()
    assert not j.has_messages_in_flight()


def test_journal_restore_state_corrupted(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    headers = [("header_0", b"value_0"), ("header_1", b"value_1")]
    for i in range(0, 10):
        message = f"message {i}".encode("utf-8")
        if (i % 3) == 0:
            j.queue_message(message, headers=headers)
        else:
            j.queue_message(message)

    for i in range(0, 10):
        m = j.get_next_message_to_send()
        if (i % 2) == 0:
            j.mark_message_sent(m[0])

    del j
    # capture the raw journal for later manipulation
    with open(journal_path, "rb") as journal_file:
        journal_data = journal_file.read()

    # the unmodified journal should be readable
    j = PublicationJournal(journal_path)
    assert j.has_messages_to_send()
    assert len(j.messages_to_send) == 5
    assert not j.has_messages_in_flight()
    del j

    # test that corruption in any byte is detected
    for i in range(0, len(journal_data)):
        corrupted_data = bytearray(journal_data)
        corrupted_data[i] ^= 0b01010101  # flip some bits
        with open(journal_path, "wb") as journal_file:
            journal_file.write(corrupted_data)
        with pytest.raises(RuntimeError):
            try:
                j = PublicationJournal(journal_path)
            except RuntimeError as e:
                print(i, e)
                raise

    # test that invalid unicode in message header keys is detected
    corrupted_data = bytearray(journal_data).replace(b"header_0",
                                                     b"\xc3\x28\xa0\xa1\xf0\x28\x8c\xbc")
    with open(journal_path, "wb") as journal_file:
        journal_file.write(corrupted_data)
    with pytest.raises(RuntimeError):
        try:
            j = PublicationJournal(journal_path)
        except RuntimeError as e:
            print("invalid unicode message header", e)
            raise


def test_journal_restore_state_truncated(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    headers = [(b"header_0", b"value_0"), (b"header_1", b"value_1")]
    for i in range(0, 10):
        message = f"message {i}".encode("utf-8")
        if (i % 3) == 0:
            j.queue_message(message, headers=headers)
        else:
            j.queue_message(message)

    for i in range(0, 10):
        m = j.get_next_message_to_send()
        if (i % 2) == 0:
            j.mark_message_sent(m[0])

    del j
    # capture the raw journal for later manipulation
    with open(journal_path, "rb") as journal_file:
        journal_data = journal_file.read()

    # these are the offsets of the ends of the records
    # if the file format or test data is changed, these must be updated
    # truncating the file between records is valid/undetectable, so we should not test these
    valid_break_points = [119, 176, 233, 352, 409, 466, 585, 642, 699, 818,
                          850, 882, 914, 946, 978]
    # test that truncation at any byte (in a record) is detected
    for i in range(1, len(journal_data)):
        if i in valid_break_points:
            continue
        with open(journal_path, "wb") as journal_file:
            journal_file.write(journal_data[0:i])
        with pytest.raises(RuntimeError):
            try:
                j = PublicationJournal(journal_path)
            except RuntimeError as e:
                print(i, e)
                raise
            print(f"Failure: Truncation at byte {i} should be detected")


def test_journal_restore_duplicate_sequence_number_to_send(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    # directly use internal recording function to generate records with valid checksums
    # but dubious meanings
    test_message = b"data"
    rbody = BytesIO()
    rbody.write(PublicationJournal.encode_int(58))  # sequence number
    rbody.write(PublicationJournal.encode_int(len(test_message)))  # data length
    rbody.write(test_message)  # data
    rbody.write(PublicationJournal.encode_int(0))  # no headers
    j._write_record(PublicationJournal.msg_record_type, rbody.getvalue())
    # record the same message again, duplicating the sequence number
    j._write_record(PublicationJournal.msg_record_type, rbody.getvalue())
    del j

    with pytest.raises(RuntimeError) as excinfo:
        j = PublicationJournal(journal_path)
    assert "Duplicate message sequence number" in str(excinfo.value)

    journal_path = tmpdir.join("journal2")
    j = PublicationJournal(journal_path)
    test_message2 = b"other data"
    rbody2 = BytesIO()
    rbody2.write(PublicationJournal.encode_int(58))  # sequence number
    rbody2.write(PublicationJournal.encode_int(len(test_message2)))  # data length
    rbody2.write(test_message2)  # data
    rbody2.write(PublicationJournal.encode_int(0))  # no headers
    j._write_record(PublicationJournal.msg_record_type, rbody.getvalue())
    # record another message but with the same sequence number
    j._write_record(PublicationJournal.msg_record_type, rbody2.getvalue())
    del j

    with pytest.raises(RuntimeError) as excinfo:
        j = PublicationJournal(journal_path)
    assert "Duplicate message sequence number" in str(excinfo.value)


def test_journal_restore_too_short_message_record(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    # a message record must contain the sequence number, mesage length, an header count
    # requiring 3*8 = 24 bytes. Anything shorter cannot be valid.
    j._write_record(PublicationJournal.msg_record_type, b"tooshort")
    del j

    with pytest.raises(RuntimeError) as excinfo:
        j = PublicationJournal(journal_path)
    assert "too small to conain required data for record" in str(excinfo.value)


def test_journal_restore_mismatched_body_length(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    # directly use internal recording function to generate records with valid checksums
    # but dubious meanings
    test_message = b"somedata"
    rbody = BytesIO()
    rbody.write(PublicationJournal.encode_int(0))  # sequence number
    # use wrong data length
    rbody.write(PublicationJournal.encode_int(len(test_message) * 2))
    rbody.write(test_message)  # data
    rbody.write(PublicationJournal.encode_int(0))  # no headers
    j._write_record(PublicationJournal.msg_record_type, rbody.getvalue())
    del j

    with pytest.raises(RuntimeError) as excinfo:
        j = PublicationJournal(journal_path)
    assert "Claimed message data length" in str(excinfo.value)
    assert "exceeds record body length" in str(excinfo.value)


def test_journal_restore_missing_headers(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    # directly use internal recording function to generate records with valid checksums
    # but dubious meanings
    test_message = b"data"
    rbody = BytesIO()
    rbody.write(PublicationJournal.encode_int(0))  # sequence number
    rbody.write(PublicationJournal.encode_int(len(test_message)))  # data length
    rbody.write(test_message)  # data
    # claim a bunch of headers, but then don't include any
    rbody.write(PublicationJournal.encode_int(56))
    j._write_record(PublicationJournal.msg_record_type, rbody.getvalue())
    del j

    with pytest.raises(RuntimeError) as excinfo:
        j = PublicationJournal(journal_path)
    assert "Claimed number of message headers" in str(excinfo.value)
    assert "exceeds remaining space in record body" in str(excinfo.value)


def test_journal_restore_sent_unknown_sequence_number(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    # directly use internal recording function to generate records with valid checksums
    # but dubious meanings
    # record that a message was sent without it being previously mentioned
    j._write_record(PublicationJournal.sent_record_type, PublicationJournal.encode_int(56))
    del j

    with pytest.raises(RuntimeError) as excinfo:
        j = PublicationJournal(journal_path)
    assert "Record of sent message" in str(excinfo.value)
    assert "which did not previously appear" in str(excinfo.value)


def test_journal_restore_duplicate_send(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    # directly use internal recording function to generate records with valid checksums
    # but dubious meanings
    test_message = b"data"
    rbody = BytesIO()
    rbody.write(PublicationJournal.encode_int(17))  # sequence number
    rbody.write(PublicationJournal.encode_int(len(test_message)))  # data length
    rbody.write(test_message)  # data
    rbody.write(PublicationJournal.encode_int(0))  # no headers
    j._write_record(PublicationJournal.msg_record_type, rbody.getvalue())
    # record that the message was sent
    j._write_record(PublicationJournal.sent_record_type, PublicationJournal.encode_int(17))
    # record that the message was sent _again_
    j._write_record(PublicationJournal.sent_record_type, PublicationJournal.encode_int(17))
    del j

    with pytest.raises(RuntimeError) as excinfo:
        j = PublicationJournal(journal_path)
    assert "Record of sent message" in str(excinfo.value)
    assert "which did not previously appear" in str(excinfo.value)


def test_journal_restore_duplicate_bogus_record_type(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)
    # use a record type which is not defined
    bogus_record_type = 46000
    j._write_record(bogus_record_type, b"irrelevant")
    del j

    with pytest.raises(RuntimeError) as excinfo:
        j = PublicationJournal(journal_path)
    assert f"Invalid record type ({bogus_record_type})" in str(excinfo.value)


def test_journal_restore_read_failure(tmpdir):
    def read_fail(*args, **kwargs):
        raise OSError(errno.EIO, os.strerror(errno.EIO))
    file_mock = MagicMock()
    file_mock.read = read_fail
    file_mock.__enter__ = MagicMock(return_value=file_mock)
    open_mock = MagicMock(return_value=file_mock)

    journal_path = tmpdir.join("journal")
    with open(journal_path, 'w'):
        pass  # touch the file
    with patch("builtins.open", open_mock):
        with pytest.raises(RuntimeError) as excinfo:
            j = PublicationJournal(journal_path)
        assert "Journal corrupted: Unable to read" in str(excinfo.value)


def test_journal_get_delivery_callback_unqueued_message(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    # can't get a callback for a message which isn't yet queued
    with pytest.raises(RuntimeError) as excinfo:
        j.get_delivery_callback(22)
    assert "Cannot produce a delivery callback for message" in str(excinfo.value)
    assert "which is not in flight" in str(excinfo.value)

    # can't get a callback for a message which has already been sent
    j.queue_message(b"message 0")
    s0 = j.get_next_message_to_send()
    j.mark_message_sent(s0[0])

    with pytest.raises(RuntimeError) as excinfo:
        j.get_delivery_callback(s0[0])
    assert "Cannot produce a delivery callback for message" in str(excinfo.value)
    assert "which is not in flight" in str(excinfo.value)


def test_journal_delivery_callback_mark_sent(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    j.queue_message(b"message 0")
    s0 = j.get_next_message_to_send()
    callback = j.get_delivery_callback(s0[0])
    assert j.has_messages_in_flight()

    msg_obj = MagicMock()
    msg_obj.error = MagicMock(return_value=None)  # indicate no error if asked
    callback(None, msg_obj)  # no error and the 'message'
    assert not j.has_messages_in_flight()


def test_journal_delivery_callback_requeue(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    j.queue_message(b"message 0")
    s0 = j.get_next_message_to_send()
    callback = j.get_delivery_callback(s0[0])
    assert j.has_messages_in_flight()
    assert not j.has_messages_to_send()

    msg_obj = MagicMock()
    msg_obj.error = MagicMock(return_value="bad!")  # some error message

    # invoking the callback with an error should cause the message to be requeued
    callback("disaster", msg_obj)  # an error and the 'message'
    assert not j.has_messages_in_flight()
    assert j.has_messages_to_send()

    s0 = j.get_next_message_to_send()  # pretend we're retrying to send

    callback(None, msg_obj)  # no direct error, but the 'message' contains one
    assert not j.has_messages_in_flight()
    assert j.has_messages_to_send()


class TrialLock():
    def __init__(self):
        self.held = False
        self.was_locked = False

    def __enter__(self):
        assert not self.held
        self.held = True
        self.was_locked = True

    def __exit__(self, exc_type, exc_value, exc_traceback):
        assert self.held
        self.held = False


def test_journal_delivery_callback_locking(tmpdir):
    journal_path = tmpdir.join("journal")
    j = PublicationJournal(journal_path)

    myLock = TrialLock()

    def wrap(func):
        def require_lock(*args, **kwargs):
            assert myLock.held, "Lock must be held for this operation"
            return func(*args, **kwargs)
        return require_lock

    j.queue_message(b"message 0")
    s0 = j.get_next_message_to_send()
    callback = j.get_delivery_callback(s0[0], myLock)

    msg_obj = MagicMock()
    msg_obj.error = MagicMock(return_value=None)

    with patch("hop.robust_publisher.PublicationJournal.mark_message_sent",
               wrap(PublicationJournal.mark_message_sent)), \
            patch("hop.robust_publisher.PublicationJournal.requeue_message",
                  wrap(PublicationJournal.requeue_message)):
        assert not myLock.was_locked
        callback("error!", msg_obj)  # simulate sending failure
        assert not myLock.held, "Lock should be released"
        assert myLock.was_locked, "Lock should have been held"

        # 'get' the message to send again
        s0_2 = j.get_next_message_to_send()
        assert s0_2[0] == s0[0]

        myLock.was_locked = False
        callback(None, msg_obj)  # simulate sending success
        assert not myLock.held, "Lock should be released"
        assert myLock.was_locked, "Lock should have been held"


def test_journal_error_callback():
    # this is just required to not throw
    PublicationJournal.error_callback("terrible error!")


########################################


def makeKafkaException(name="error_name", reason="error reason"):
    err = MagicMock()
    err.name = MagicMock(return_value=name)
    err.reason = MagicMock(return_value=reason)
    err.retriable = MagicMock(return_value=True)
    err.fatal = MagicMock(return_value=False)
    return KafkaException(err)


class FakeProducer:
    def __init__(self, immediate_failure=False, poll_failure=False):
        self.delivery_callbacks = []
        self.messages_written = []
        # need to coordinate access due to RobustProducer's background thread
        self.lock = threading.Lock()
        # need this to allow ._producer._producer.poll()
        self._producer = MagicMock()
        self._producer._producer = MagicMock()
        if poll_failure:
            logger.debug("poll should fail")
            exc = makeKafkaException()

            def fail_poll(delay):
                time.sleep(delay)
                raise exc

            self._producer._producer.poll = fail_poll
            logger.debug(f" poll is {self._producer._producer.poll}")
        self._immediate_failure = immediate_failure

    def invoke_all_callbacks(self, err, msg):
        with self.lock:
            for callback in self.delivery_callbacks:
                callback(err, msg)
            self.delivery_callbacks.clear()

    def write_raw(self, packed_message, headers, delivery_callback):
        logger.debug(f" FakeProducer.write_raw called with {packed_message}, {headers}")
        if self._immediate_failure:
            self._immediate_failure = False  # stop failing after the first time
            raise makeKafkaException()
        with self.lock:
            self.messages_written.append((packed_message, headers))
            self.delivery_callbacks.append(delivery_callback)

    def flush(self):
        logger.debug(f" FakeProducer.flush called with {len(self.delivery_callbacks)} "
                     "callback(s) in queue")
        # simulate any remaining messages being sent successfully
        msg_no_err = MagicMock()
        msg_no_err.error = MagicMock(return_value=None)
        msg_no_err.latency = MagicMock(return_value=0.1)
        self.invoke_all_callbacks(None, msg_no_err)

    def close(self):
        logger.debug(" FakeProducer.close called")
        self.flush()

    def stop_failing_poll(self):
        self._producer._producer.poll = MagicMock(return_value=None)


# build pointless layers surrounding the hop.io.Producer class
def makeStream(*args, **kwargs):
    opener = MagicMock()
    opener.open = MagicMock(return_value=FakeProducer(*args, **kwargs))
    return MagicMock(return_value=opener)


def test_rpublisher_empty_journal(tmpdir):
    journal_path = tmpdir.join("journal")
    url = "kafka://example.com/topic"

    with patch("hop.io.Stream", makeStream()) as steam_middleman:
        with RobustProducer(url, journal_path=journal_path) as pub:
            pub.write("a message")

            # The publisher will spin in _do_send as long as the state of sent messages is
            # indeterminate. So, spin here in a thread-safe way until the callbacks show up, then
            # invoke them to let it complete.
            while True:
                time.sleep(0.01)
                with pub._stream.lock:
                    msg_count = len(pub._stream.messages_written)
                if msg_count == 1:
                    pub._stream.flush()
                    break
        assert hop.io.Producer.pack("a message", None) in pub._stream.messages_written


def test_rpublisher_existing_journal(tmpdir):
    journal_path = tmpdir.join("journal")
    url = "kafka://example.com/topic"
    messages = [b"message 0", b"message 1"]

    j = PublicationJournal(journal_path)
    for message in messages:
        j.queue_message(message)
    del j

    with patch("hop.io.Stream", makeStream()) as steam_middleman:
        with RobustProducer(url, journal_path=journal_path) as pub:
            while True:
                time.sleep(0.01)
                with pub._stream.lock:
                    msg_count = len(pub._stream.messages_written)
                if msg_count == 2:
                    pub._stream.flush()
                    break
        # each message previously persisted in the journal should be sent
        sent_messages = [item[0] for item in pub._stream.messages_written]
        for message in messages:
            assert message in sent_messages


def test_rpublisher_immediate_send_fail(tmpdir):
    journal_path = tmpdir.join("journal")
    url = "kafka://example.com/topic"

    with patch("hop.io.Stream", makeStream(immediate_failure=True)) as steam_middleman:
        with RobustProducer(url, journal_path=journal_path) as pub:
            pub.write("a message")

            while True:
                time.sleep(0.01)
                with pub._stream.lock:
                    msg_count = len(pub._stream.messages_written)
                if msg_count == 1:
                    pub._stream.flush()
                    break
        assert hop.io.Producer.pack("a message", None) in pub._stream.messages_written


def test_rpublisher_poll_fail(tmpdir):
    print("test_rpublisher_poll_fail")
    journal_path = tmpdir.join("journal")
    url = "kafka://example.com/topic"

    with patch("hop.io.Stream", makeStream(poll_failure=True)) as steam_middleman:
        with RobustProducer(url, journal_path=journal_path) as pub:
            pub.write("a message")

            while True:
                logger.debug("wating for message to be sent")
                time.sleep(0.01)
                with pub._stream.lock:
                    msg_count = len(pub._stream.messages_written)
                if msg_count == 1:
                    logger.debug("message is visible, waiting to allow poll to proceed")
                    time.sleep(0.01)
                    logger.debug("stopping poll failures")
                    pub._stream.stop_failing_poll()
                    pub._stream.flush()
                    break
            logger.debug("with body done")
        assert hop.io.Producer.pack("a message", None) in pub._stream.messages_written
