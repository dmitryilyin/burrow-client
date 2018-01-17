#!/usr/bin/env python

from requests import get
from sys import stdout
from datetime import datetime
import argparse
from re import match
from prettytable import PrettyTable


class BurrowClient(object):
    """
    Burrow API Client and response data formatter
    """
    def __init__(self, url='http://127.0.0.1:8000', api='v3/kafka', verbose=False, debug=False):
        """
        Args:
            url(str): Base url of the Burrow server 
            api(str): Burrow server endpoint prefix 
            verbose(bool): Show extended reports 
            debug(bool): Show HTTP requests
        """
        self.api = api
        self.url = url

        self.verbose = verbose
        self.debug = debug
        self.decimal = False
        self.enable_clusters = False
        self.enable_topics = False
        self.enable_consumers = False
        self.enable_consumer_status = False
        self.enable_consumer_partitions = False
        self.filter_topic = None
        self.filter_consumer = None

        self._clusters = None
        self._cluster_details = {}
        self._consumers = {}
        self._topics = {}
        self._topic_details = {}
        self._consumer_details = {}
        self._consumer_status = {}

    def address(self, *paths):
        """
        Get the full address for the specified path components
        Args:
            *paths(list): A list of path components 
        Returns:
            str
        """
        address_list = [self.url, self.api]
        address_list += list(paths)
        return '/'.join(address_list)

    def request(self, *paths):
        """
        Send the HTTP request to the URL specified by path components
        Args:
            *paths(str): A list of path component strings 
        Returns:
            dict
        """
        address = self.address(*paths)
        if self.debug:
            self.output("=> GET: {address}".format(address=address))
        response = get(address)
        data = response.json()
        return data

    @staticmethod
    def timestamp_to_date(timestamp):
        """
        Convert miliseconds timestamp to the date string.
        Returns '?' on error.
        Args:
            timestamp(str): Timestamp in miliseconds 
        Returns:
            str
        """
        try:
            timestamp = int(timestamp) / 1000
            date = datetime.fromtimestamp(timestamp)
            return str(date)
        except RuntimeError:
            return '?'

    @staticmethod
    def timestamp_to_age(timestamp):
        """
        Convert miliseconds timestamp to the age string.
        Returns '?' on error.
        Args:
            timestamp(str): Timestamp in miliseconds 
        Returns:
            str
        """
        try:
            timestamp = int(timestamp) / 1000
            timestamp_date = datetime.fromtimestamp(timestamp)
            current_date = datetime.now()
            delta_date = current_date - timestamp_date
            return str(delta_date)
        except RuntimeError:
            return '?'

    def format_number(self, number):
        """
        Format and integer number optionally adding decimal separators.
        Args:
            number(str,int): Input number 
        Returns:
            str
        """
        number = int(number)
        if self.decimal:
            return '{number:,}'.format(number=number)
        else:
            return str(number)

    @property
    def clusters(self):
        """
        Get the list of configured clusters names
        Returns:
            list
        """
        if self._clusters:
            return self._clusters
        data = self.request()
        clusters = data.get('clusters', [])
        if clusters:
            self._clusters = clusters
        return clusters

    def cluster_details(self, cluster):
        """
        Get the configuration details of a cluster
        Args:
            cluster(str): Cluster name 
        Returns:
            dict
        """
        key = cluster
        if key in self._cluster_details:
            return self._cluster_details[key]
        data = self.request(cluster)
        cluster_details = data.get('module', {})
        if cluster_details:
            self._cluster_details[key] = cluster_details
        return cluster_details

    def topics(self, cluster):
        """
        Get the list of topic in the specified cluster
        Args:
            cluster(str): Cluster name 
        Returns:
            list
        """
        key = cluster
        if key in self._topics:
            return self._topics[key]
        data = self.request(cluster, 'topic')
        topics = data.get('topics', [])
        if topics:
            self._topics[key] = topics
        return topics

    def topic_details(self, cluster, topic):
        """
        Get the details about a topic in a cluster
        Args:
            cluster(str): Cluster name 
            topic(str): Topic name 
        Returns:
            dict
        """
        key = "{cluster}-{topic}".format(
            cluster=cluster,
            topic=topic,
        )
        if key in self._topic_details:
            return self._topic_details[key]
        data = self.request(cluster, 'topic', topic)
        topic_details = data.get('offsets', [])
        if topic_details:
            self._topic_details[key] = topic_details
        return topic_details

    def consumers(self, cluster):
        """
        Get a list of consumer group names for the specified cluster
        Args:
            cluster(str): Cluster name  
        Returns:
            list
        """
        key = cluster
        if key in self._consumers:
            return self._consumers[key]
        data = self.request(cluster, 'consumer')
        consumers = data.get('consumers', [])
        if consumers:
            self._consumers[key] = consumers
        return consumers

    def consumer_details(self, cluster, consumer):
        """
        Get details about the specified consumer group in a cluster
        Args:
            cluster(sr): Cluster name
            consumer(srt): Consumer group name
        Returns:
            dict
        """
        key = "{cluster}-{consumer}".format(
            cluster=cluster,
            consumer=consumer,
        )
        if key in self._consumer_details:
            return self._consumer_details[key]
        data = self.request(cluster, 'consumer', consumer)
        consumer_details = data.get('topics', {})
        if consumer_details:
            self._consumer_details[key] = consumer_details
        return consumer_details

    def consumer_status(self, cluster, consumer):
        """
        Get the status of the specified consumer group in a cluster
        Args:
            cluster(sr): Cluster name
            consumer(srt): Consumer group name
        Returns:
            dict
        """
        key = "{cluster}-{consumer}".format(
            cluster=cluster,
            consumer=consumer,
        )
        if key in self._consumer_status:
            return self._consumer_status[key]
        data = self.request(cluster, 'consumer', consumer, 'lag')
        consumer_status = data.get('status', {})
        if consumer_status:
            self._consumer_status[key] = consumer_status
        return consumer_status

    def match_topic(self, topic):
        """
        Check if the topic name passes filter or there is not topic filter defined.
        Args:
            topic(str): Topic name 
        Returns:
            bool
        """
        if not self.filter_topic:
            return True
        if not topic:
            return False
        return bool(match(self.filter_topic, topic))

    def match_consumer(self, consumer):
        """
        Check if the consumer group name passes filter or there is not consumer group filter defined.
        Args:
            consumer(str): Consumer group name 
        Returns:
            bool
        """
        if not self.filter_consumer:
            return True
        if not consumer:
            return False
        return bool(match(self.filter_consumer, consumer))

    def report_clusters(self):
        """
        Generate configured clusters report
        Returns:
            str
        """
        report = PrettyTable()
        report.field_names = [
            'Cluster', 'Version', 'Client ID', 'Refresh Offsets', 'Refresh Topics', 'Servers'
        ]
        report.align = 'l'
        for cluster in self.clusters:
            details = self.cluster_details(cluster)
            report.add_row([
                cluster,
                details.get("client-profile", {}).get("kafka-version", '?'),
                details.get("client-profile", {}).get("client-id", '?'),
                details.get("offset-refresh", '?'),
                details.get("topic-refresh", '?'),
                ','.join(details.get('servers', [])),
            ])
        return report.get_string() + "\n"

    def report_topics(self, cluster):
        """
        Generate topic report
        Args:
            cluster(str): Cluster name 
        Returns:
            str
        """
        report = PrettyTable()
        if self.verbose:
            report.field_names = [
                'Topic', 'Partitions', 'Offsets'
            ]
        else:
            report.field_names = [
                'Topic', 'Partitions', 'Min Offset', 'Max Offset',
            ]
        report.align = 'l'

        for topic in self.topics(cluster):
            if not self.match_topic(topic):
                continue
            offsets = [offset for offset in self.topic_details(cluster, topic)]
            partitions = len(offsets)
            max_offset = max(offsets)
            min_offset = min(offsets)
            offsets_string = ','.join([
                self.format_number(offset) for offset in offsets
            ])
            if self.verbose:
                report.add_row([
                    topic,
                    partitions,
                    offsets_string,
                ])
            else:
                report.add_row([
                    topic,
                    partitions,
                    self.format_number(min_offset),
                    self.format_number(max_offset),
                ])
        return report.get_string(sortby='Topic') + "\n"

    def report_consumers(self, cluster):
        """
        Generate consumer groups report
        Args:
            cluster(str): Cluster name 
        Returns:
            str
        """
        report = PrettyTable()
        report.field_names = [
            'Consumer Group', 'Topic', 'Owner', 'Current Lag', 'Min Offset', 'Max Offset', 'Timestamp', 'Date', 'Age'
        ]
        report.align = 'l'

        for consumer in self.consumers(cluster):
            if not self.match_consumer(consumer):
                continue
            consumer_details = self.consumer_details(cluster, consumer)
            for topic, details in consumer_details.items():
                if not self.match_topic(topic):
                    continue
                for detail in details:
                    owner = detail.get('owner', '?')
                    lag = detail.get('current-lag', '?')
                    offset_data = detail.get('offsets', [])
                    offsets = [
                        offset.get('offset', 0) for offset in offset_data
                    ]
                    timestamps = [
                        offset.get('timestamp', 0) for offset in offset_data
                    ]
                    max_offset = max(offsets)
                    min_offset = min(offsets)
                    max_timestamp = max(timestamps)
                    report.add_row([
                        consumer,
                        topic,
                        owner,
                        self.format_number(lag),
                        self.format_number(min_offset),
                        self.format_number(max_offset),
                        self.format_number(max_timestamp),
                        self.timestamp_to_date(max_timestamp),
                        self.timestamp_to_age(max_timestamp),
                    ])
        return report.get_string(sortby='Consumer Group') + "\n"

    @staticmethod
    def get_partition_stats(partition_data):
        """
        Get status from a partition data structure
        Args:
            partition_data(dict): Partition data structure 
        Returns:
            dict
        """
        topic = partition_data.get('topic', '?')
        end = partition_data.get('end', {})
        if not isinstance(end, dict):
            end = {}
        if 'current_lag' in partition_data:
            lag = partition_data.get('current_lag', 0)
        else:
            lag = end.get('lag', 0)
        offset = end.get('offset', 0)
        timestamp = end.get('timestamp', 0)
        status = partition_data.get('status', '?')
        owner = partition_data.get('owner', '?')
        partition = partition_data.get('partition', '?')
        return {
            'topic': topic,
            'lag': lag,
            'offset': offset,
            'timestamp': timestamp,
            'status': status,
            'owner': owner,
            'partition': partition,
        }

    def report_consumer_status(self, cluster):
        """
        Generate consumer group status report
        Args:
            cluster(str): Cluster name 
        Returns:
            str
        """
        report = PrettyTable()
        if self.verbose:
            report.field_names = [
                'Consumer Group', 'Status', 'Current Lag', 'Total Lag', 'Offset', 'Timestamp', 'Date', 'Age', 'Owners', 'Topics'
            ]
        else:
            report.field_names = [
                'Consumer Group', 'Status', 'Current Lag', 'Total Lag', 'Offset', 'Timestamp', 'Date', 'Age'
            ]

        report.align = 'l'
        for consumer in self.consumers(cluster):
            if not self.match_consumer(consumer):
                continue
            consumer_status = self.consumer_status(cluster, consumer)
            if 'maxlag' in consumer_status and consumer_status['maxlag']:
                maxlag_stats = self.get_partition_stats(consumer_status['maxlag'])
                lag = maxlag_stats.get('lag', 0)
            else:
                lag = 0
            status = consumer_status.get('status', '?')
            total_lag = consumer_status.get('totallag', 0)
            partitions = consumer_status.get('partitions', [])
            topics = {}
            owners = set()
            max_offset = 0
            max_timestamp = 0
            for partition_data in partitions:
                partition_stats = self.get_partition_stats(partition_data)
                if not self.match_topic(partition_stats['topic']):
                    continue
                offset = partition_stats['offset']
                timestamp = partition_stats['timestamp']
                topic = partition_stats['topic']
                owner = partition_stats['owner']
                if offset > max_offset:
                    max_offset = offset
                if timestamp > max_timestamp:
                    max_timestamp = timestamp
                if topic:
                    if topic not in topics:
                        topics[topic] = 1
                    else:
                        topics[topic] += 1
                if owner:
                    owners.add(owner)
            if self.verbose:
                owners_string = ','.join(sorted(list(owners)))
                topics_string = ','.join([
                    '{topic}[{partitions}]'.format(
                        topic=topic,
                        partitions=partitions,
                    ) for topic, partitions in sorted(topics.items())
                ])
                report.add_row([
                    consumer,
                    status,
                    self.format_number(lag),
                    self.format_number(total_lag),
                    self.format_number(max_offset),
                    self.format_number(max_timestamp),
                    self.timestamp_to_date(max_timestamp),
                    self.timestamp_to_age(max_timestamp),
                    owners_string,
                    topics_string,
                ])
            else:
                report.add_row([
                    consumer,
                    status,
                    self.format_number(lag),
                    self.format_number(total_lag),
                    self.format_number(max_offset),
                    self.format_number(max_timestamp),
                    self.timestamp_to_date(max_timestamp),
                    self.timestamp_to_age(max_timestamp),
                ])
        return report.get_string(sortby='Consumer Group') + "\n"

    def report_consumer_partitions(self, cluster):
        """
        Generate consumer group partitions report
        Args:
            cluster(str): Cluster name 
        Returns:
            str
        """
        report = PrettyTable()
        report.field_names = [
            'Consumer Group', 'Topic', 'Partition', 'Status', 'Lag', 'Owner', 'Offset', 'Timestamp', 'Date', 'Age'
        ]
        report.align = 'l'

        for consumer in self.consumers(cluster):
            if not self.match_consumer(consumer):
                continue
            consumer_data = self.consumer_status(cluster, consumer)
            partitions = consumer_data.get('partitions', [])
            for partition_data in partitions:
                partition_stats = self.get_partition_stats(partition_data)
                if not self.match_topic(partition_stats['topic']):
                    continue
                report.add_row([
                    consumer,
                    partition_stats['topic'],
                    partition_stats['partition'],
                    partition_stats['status'],
                    self.format_number(partition_stats['lag']),
                    partition_stats['owner'],
                    self.format_number(partition_stats['offset']),
                    self.format_number(partition_stats['timestamp']),
                    self.timestamp_to_date(partition_stats['timestamp']),
                    self.timestamp_to_age(partition_stats['timestamp']),
                ])
        return report.get_string(sortby='Consumer Group') + "\n"

    def report(self):
        """
        Generate the full report with enabled sections.
        Returns:
            str
        """
        report = ''
        if self.enable_clusters:
            report += self.report_clusters()
        for cluster in self.clusters:
            if self.enable_topics or self.enable_consumers or \
                    self.enable_consumer_status or self.enable_consumer_partitions:
                report += "\n### Cluster: {cluster} ###\n\n".format(cluster=cluster)
            if self.enable_topics:
                report += self.report_topics(cluster)
            if self.enable_consumers:
                report += self.report_consumers(cluster)
            if self.enable_consumer_status:
                report += self.report_consumer_status(cluster)
            if self.enable_consumer_partitions:
                report += self.report_consumer_partitions(cluster)
        return report

    @staticmethod
    def output(message):
        """
        Output test message
        Args:
            message(str): text to output 
        Returns:
            None
        """
        if not isinstance(message, str):
            message = str(message)
        if not message.endswith("\n"):
            message += "\n"
        stdout.write(message)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description='The Burrow API CLI tool'
    )
    arg_parser.add_argument(
        "-u", "--url",
        default="http://127.0.0.1:8000",
        help="Burrow server root URL",
    )
    arg_parser.add_argument(
        "-a", "--api",
        default='v3/kafka',
        help="Burrow API prefix after the root URL"
    )
    arg_parser.add_argument(
        "-l", "--clusters",
        default=False,
        help="Show all configured Kafka clusters",
        action="store_true"
    )
    arg_parser.add_argument(
        "-t", "--topics",
        default=False,
        help="Show the list of Kafka topic offsets",
        action="store_true"
    )
    arg_parser.add_argument(
        "-c", "--consumers",
        default=False,
        help="Show the list of Kafka consumers",
        action="store_true"
    )
    arg_parser.add_argument(
        "-s", "--status",
        default=False,
        help="Show Kafka consumer status summary",
        action="store_true"
    )
    arg_parser.add_argument(
        "-p", "--partitions",
        default=False,
        help="Show Kafka consumer status by every partition",
        action="store_true"
    )
    arg_parser.add_argument(
        "-T", "--filter_topic",
        default=None,
        help="Filter report output by topic name regular expression"
    )
    arg_parser.add_argument(
        "-C", "--filter_consumer",
        default=None,
        help="Filter report output by consumer group name regular expression"
    )
    arg_parser.add_argument(
        "-v", "--verbose",
        default=False,
        help="Show extended versions of reports",
        action="store_true"
    )
    arg_parser.add_argument(
        "-d", "--debug",
        default=False,
        help="Show HTTP requests to the Burrow API",
        action="store_true"
    )
    arg_parser.add_argument(
        "-e", "--decimal",
        default=False,
        help="Show decimal separator commas in large numbers",
        action="store_true"
    )

    args = arg_parser.parse_args()

    if not (args.clusters or args.topics or args.consumers or args.status or args.partitions):
        args.status = True

    bc = BurrowClient(url=args.url, api=args.api)
    bc.verbose = args.verbose
    bc.debug = args.debug
    bc.decimal = args.decimal

    bc.filter_topic = args.filter_topic
    bc.filter_consumer = args.filter_consumer

    bc.enable_clusters = args.clusters
    bc.enable_topics = args.topics
    bc.enable_consumers = args.consumers
    bc.enable_consumer_status = args.status
    bc.enable_consumer_partitions = args.partitions

    bc.output(bc.report())
