from . import cli
from .auth import load_auth
from .auth import select_matching_auth
from .io import _generate_group_id, parse_kafka_url
from kafka import KafkaConsumer


def _main(args):
    """List available topics.

    """
    username, broker_addresses, query_topics = parse_kafka_url(args.url)
    if len(broker_addresses) > 1:
        raise ValueError("Multiple broker addresses are not supported")
    user_auth = None
    if not args.no_auth:
        credentials = load_auth()
        user_auth = select_matching_auth(credentials, broker_addresses[0], username)
    group_id = _generate_group_id(username, 10)
    config = {
        "bootstrap_servers": broker_addresses,
    }
    if user_auth is not None:
        config.update(user_auth())
    consumer = KafkaConsumer(**config)
    valid_topics = consumer.topics()
    if query_topics is not None:
        for topic in valid_topics:
            if topic not in query_topics:
                del valid_topics[topic]

    if len(valid_topics) == 0:
        print("No accessible topics")
    else:
        print("Accessible topics:")
        for topic in sorted(valid_topics):
            print(f" {topic}")


def _add_parser_args(parser):
    cli.add_client_opts(parser)
