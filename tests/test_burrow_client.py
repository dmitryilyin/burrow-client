import unittest
import json
from burrow_client import BurrowClient
from unittest.mock import patch

RESPONSE_CLUSTERS = """
{
    "error": false,
    "message": "cluster list returned",
    "clusters": [
        "clustername1",
        "clustername2"
    ],
    "request": {
        "uri": "/v2/kafka",
        "host": "responding.host.example.com"
    }
}
"""

RESPONSE_CLUSTER_DETAILS = """
{
    "error": false,
    "message": "cluster module detail returned",
    "module": {
        "class-name": "kafka",
        "client-profile": {
            "client-id": "burrow-i001",
            "kafka-version": "0.8",
            "name": "default"
        },
        "offset-refresh": 30,
        "servers": [
            "kafka01.example.com:10251",
            "kafka02.example.com:10251",
            "kafka03.example.com:10251"
        ],
        "topic-refresh": 120
    },
    "request": {
        "host": "responding.host.example.com",
        "url": "/v3/kafka/tracking"
    }
}
"""

DATA_CLUSTER_DETAILS = {
    "class-name": "kafka",
    "client-profile": {
        "client-id": "burrow-i001",
        "kafka-version": "0.8",
        "name": "default"
    },
    "offset-refresh": 30,
    "servers": [
        "kafka01.example.com:10251",
        "kafka02.example.com:10251",
        "kafka03.example.com:10251"
    ],
    "topic-refresh": 120
}

RESPONSE_CONSUMERS = """
{
    "error": false,
    "message": "consumer list returned",
    "consumers": [
        "group1",
        "group2"
    ],
    "request": {
        "url": "/v3/kafka/clustername/consumer",
        "host": "responding.host.example.com"
    }
}
"""

RESPONSE_TOPICS = """
{
    "error": false,
    "message": "broker topic list returned",
    "topics": [
        "topicA",
        "topicB"
    ],
    "request": {
        "url": "/v3/kafka/clustername/topic",
        "host": "responding.host.example.com"
    }
}
"""

RESPONSE_TOPIC_DETAILS = """
{
    "error": false,
    "message": "broker topic offsets returned",
    "offsets": [
        2290903,
        2898892,
        3902933,
        2328823
    ],
    "request": {
        "url": "/v3/kafka/clustername/topic/topicname",
        "host": "responding.host.example.com"
    }
}
"""

RESPONSE_CONSUMER_DETAILS = """
{
    "error": false,
    "message": "consumer detail returned",
    "request": {
        "host": "responding.host.example.com",
        "url": "/v3/kafka/tracking/consumer/megaphone-bps"
    },
    "topics": {
        "ConsumedTopicName": [
            {
                "current-lag": 0,
                "offsets": [
                    {
                        "lag": 0,
                        "offset": 2526,
                        "timestamp": 1511200836090
                    },
                    {
                        "lag": 0,
                        "offset": 2527,
                        "timestamp": 1511321306786
                    }
                ],
                "owner": ""
            }
        ]
    }
}
"""

DATA_CONSUMER_DETAILS = {
    "ConsumedTopicName": [
        {
            "current-lag": 0,
            "offsets": [
                {
                    "lag": 0,
                    "offset": 2526,
                    "timestamp": 1511200836090
                },
                {
                    "lag": 0,
                    "offset": 2527,
                    "timestamp": 1511321306786
                }
            ],
            "owner": ""
        }
    ]
}

RESPONSE_CONSUMER_STATUS = """
{
  "error": false,
  "message": "consumer group status returned",
  "status": {
    "cluster": "clustername",
    "group": "groupname",
    "status": "WARN",
    "complete": 1.0,
    "maxlag": {
      "complete": 1,
      "current_lag": 0,
      "end": {
        "lag": 25,
        "offset": 2542,
        "timestamp": 1511780580382
      },
      "owner": "",
      "partition": 0,
      "start": {
        "lag": 20,
        "offset": 2526,
        "timestamp": 1511200836090
      },
      "status": "WARN",
      "topic": "topicA"
    },
    "partitions": [
      {
        "complete": 1,
        "current_lag": 0,
        "end": {
          "lag": 25,
          "offset": 2542,
          "timestamp": 1511780580382
        },
        "owner": "",
        "partition": 0,
        "start": {
          "lag": 20,
          "offset": 2526,
          "timestamp": 1511200836090
        },
        "status": "WARN",
        "topic": "topicA"
      }
    ]
  },
  "request": {
    "url": "/v3/kafka/clustername/consumer/groupname/status",
    "host": "responding.host.example.com"
  }
}
"""

DATA_CONSUMER_STATUS = {
    "cluster": "clustername",
    "group": "groupname",
    "status": "WARN",
    "complete": 1.0,
    "maxlag": {
        "complete": 1,
        "current_lag": 0,
        "end": {
            "lag": 25,
            "offset": 2542,
            "timestamp": 1511780580382
        },
        "owner": "",
        "partition": 0,
        "start": {
            "lag": 20,
            "offset": 2526,
            "timestamp": 1511200836090
        },
        "status": "WARN",
        "topic": "topicA"
    },
    "partitions": [
        {
            "complete": 1,
            "current_lag": 0,
            "end": {
                "lag": 25,
                "offset": 2542,
                "timestamp": 1511780580382
            },
            "owner": "",
            "partition": 0,
            "start": {
                "lag": 20,
                "offset": 2526,
                "timestamp": 1511200836090
            },
            "status": "WARN",
            "topic": "topicA"
        }
    ]
}


def request_values(*args):
    if args == ():
        return json.loads(RESPONSE_CLUSTERS)
    elif args == ('clustername1',) or args == ('clustername2',):
        return json.loads(RESPONSE_CLUSTER_DETAILS)
    elif args == ('clustername1', 'topic'):
        return json.loads(RESPONSE_TOPICS)
    elif args == ('clustername1', 'topic', 'topicA') or args == ('clustername1', 'topic', 'topicB'):
        return json.loads(RESPONSE_TOPIC_DETAILS)
    elif args == ('clustername1', 'consumer'):
        return json.loads(RESPONSE_CONSUMERS)
    elif args == ('clustername1', 'consumer', 'group1') or args == ('clustername1', 'consumer', 'group2'):
        return json.loads(RESPONSE_CONSUMER_DETAILS)
    elif args == ('clustername1', 'consumer', 'group1', 'lag') or args == ('clustername1', 'consumer', 'group2', 'lag'):
        return json.loads(RESPONSE_CONSUMER_STATUS)
    else:
        raise RuntimeError("Unknown argument: {args}".format(args=args.__repr__()))


class TestBurrowClientAPI(unittest.TestCase):
    def setUp(self):
        self.client = BurrowClient()

    def tearDown(self):
        del self.client

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_clusters(self, mock1):
        self.assertEquals(self.client.clusters, ['clustername1', 'clustername2'])
        mock1.assert_called_once_with()

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_cluster_details(self, mock1):
        self.assertEquals(self.client.cluster_details('clustername1'), DATA_CLUSTER_DETAILS)
        mock1.assert_called_once_with('clustername1')

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_topics(self, mock1):
        self.assertEquals(self.client.topics('clustername1'), ['topicA', 'topicB'])
        mock1.assert_called_once_with('clustername1', 'topic')

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_topic_details(self, mock1):
        self.assertEquals(self.client.topic_details('clustername1', 'topicA'), [2290903, 2898892, 3902933, 2328823])
        mock1.assert_called_once_with('clustername1', 'topic', 'topicA')

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_consumers(self, mock1):
        self.assertEquals(self.client.consumers('clustername1'), ['group1', 'group2'])
        mock1.assert_called_once_with('clustername1', 'consumer')

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_consumer_details(self, mock1):
        self.assertEquals(self.client.consumer_details('clustername1', 'group1'), DATA_CONSUMER_DETAILS)
        mock1.assert_called_once_with('clustername1', 'consumer', 'group1')

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_consumer_status(self, mock1):
        self.assertEquals(self.client.consumer_status('clustername1', 'group1'), DATA_CONSUMER_STATUS)
        mock1.assert_called_once_with('clustername1', 'consumer', 'group1', 'lag')


class TestBurrowClientReport(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.client = BurrowClient()

    def tearDown(self):
        del self.client

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_clusters(self, mock1):
        self.assertIsInstance(self.client.report_clusters(), str)

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_topics(self, mock1):
        self.assertIsInstance(self.client.report_topics('clustername1'), str)

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_consumers(self, mock1):
        self.assertIsInstance(self.client.report_consumers('clustername1'), str)

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_consumers(self, mock1):
        self.assertIsInstance(self.client.report_consumer_status('clustername1'), str)

    @patch('burrow_client.BurrowClient.request', side_effect=request_values)
    def test_consumers(self, mock1):
        self.assertIsInstance(self.client.report_consumer_partitions('clustername1'), str)
