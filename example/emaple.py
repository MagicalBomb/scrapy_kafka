import json

import scrapy

# 请将 MyScrapyProjectName 替换成您自己的项目名称
from MyScrapyProjectName.spiders.kafkaspider import KafkaSpider


class TestKafkaspiderSpider(KafkaSpider):
    name = 'test_kafkaspider'

    custom_settings = {
        "LOG_LEVEL": "INFO",
        ## 指定 Kafka Broker IP
        "KAFKA_BOOTSTRAP_SERVERS": ["localhost:9092"],
        "KAFKA_START_URLS_CONSUMER_PARAMS": {
            ## 指定 Kafka 消费者的组 ID
            "group_id": "test_max_poll",
            ## 如果爬虫处理数据的速度很慢，则需要降低 max_poll_records， 同时加大 max_poll_interval_ms
            ## Kafka 消费者每次从 Kafka 拉取的消息数
            "max_poll_records": 2,
            ## 告诉 Kafka 服务器超过 max_poll_interval_ms，就表示我死了，将我踢出消费组，然后进行 rebalance
            "max_poll_interval_ms": 300000,
            ## 告诉 Kafka 服务器我如果没有在 session_timeout_ms 之内发送心跳包就表示我死了
            "session_timeout_ms": 10000,
            ## 指定多久发送一次心跳。如果网络情况差，这个值需要指定的低，提高发送频率，同时加大 session_timeout_ms.
            "heartbeat_interval_ms": 3000,

            "value_deserializer": lambda data: data.decode("utf8")
        },
        ## 指定 Topic，从其中获取 scrapy 的初始请求
        "KAFKA_START_URLS_TOPIC": "raw_movie_info",
    }

    def make_request_from_data(self, data):
        self.logger.info("收到 Kafka 的消息: {}".format(data))
        return scrapy.Request(
            url="https://www.baidu.com",
        )

    def parse(self, response):
        pass
