from . import cli
from .auth import load_auth
from .auth import select_matching_auth
from .io import _generate_group_id
import adc.kafka
import adc.errors
import confluent_kafka


def _main(args):
    """List available topics.

    """
    username, broker_addresses, query_topics = adc.kafka.parse_kafka_url(args.url)
    if len(broker_addresses) > 1:
        raise ValueError("Multiple broker addresses are not supported")
    user_auth = None
    if not args.no_auth:
        credentials = load_auth()
        user_auth = select_matching_auth(credentials, broker_addresses[0], username)
    group_id = _generate_group_id(username, 10)
    config = {
        "bootstrap.servers": ",".join(broker_addresses),
        "error_cb": adc.errors.log_client_errors,
        "group.id": group_id,
    }
    if user_auth is not None:
        config.update(user_auth())
    consumer = confluent_kafka.Consumer(config)
    valid_topics = {}
    if query_topics is not None:
        for topic in query_topics:
            topic_data = consumer.list_topics(topic=topic).topics
            for topic in topic_data.keys():
                if topic_data[topic].error is None:
                    valid_topics[topic] = topic_data[topic]
    else:
        topic_results = consumer.list_topics().topics
        valid_topics = {t: d for t, d in topic_results.items() if d.error is None}
    if len(valid_topics) == 0:
        print("No accessible topics")
    else:
        print("Accessible topics:")
        for topic in sorted(valid_topics.keys()):
            print(f" {topic}")


def _add_parser_args(parser):
    cli.add_client_opts(parser)
