from kafka import KafkaConsumer
from scrapy import signals
from scrapy.exceptions import DontCloseSpider, NotConfigured
from scrapy.spiders import Spider


class KafkaMixin(object):
    """
    # 简介

    不需要直接继承本类， 继承 KafkaSpider 即可.

    # 使用方法

    必须 KafkaSpider 的子类中实现 make_request_from_data 函数

    ## 指定 Kafka Broker IP
    KAFKA_BOOTSTRAP_SERVERS = ["xx.xx.xx.xx:9092", "yy.yy.yy.yy:9092"]
    KAFKA_START_URLS_CONSUMER_PARAMS = {
        ## 指定 Kafka 消费者的组 ID
        "group_id": "XXXX",
        ## 如果爬虫处理数据的速度很慢，则需要降低 max_poll_records， 同时加大 max_poll_interval_ms
        ## Kafka 消费者每次从 Kafka 拉取的消息数
        "max_poll_records": 1,
        ## 告诉 Kafka 服务器超过 max_poll_interval_ms，就表示我死了，将我踢出消费组，然后进行 rebalance
        "max_poll_interval_ms": 300000,
        ## 告诉 Kafka 服务器我如果没有在 session_timeout_ms 之内发送心跳包就表示我死了
        "session_timeout_ms": 10000,
        ## 指定多久发送一次心跳。如果网络情况差，这个值需要指定的低，提高发送频率，同时加大 session_timeout_ms.
        "heartbeat_interval_ms": 3000,

        ## 送入 Kafka 的数据都是经过序列化的， 通过此参数指定反序列化的函数
        "value_deserializer": lambda data: json.loads(data.decode("utf8"))
    }
    ## 指定 Topic，从其中获取 scrapy 的初始请求
    KAFKA_START_URLS_TOPIC = "XXXX"
    """
    consumer = None
    kafka_topic = None
    required_params = [
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_START_URLS_CONSUMER_PARAMS",
        "KAFKA_START_URLS_TOPIC",
    ]

    def setup_kafka(self, crawler=None):
        if self.consumer is not None:
            return

        if crawler is None:
            # We allow optional crawler argument to keep backwards
            # compatibility.
            # XXX: Raise a deprecation warning.
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        settings = crawler.settings
        for s in self.required_params:
            if not settings.get(s):
                raise NotConfigured("{} 是必须的配置".format(s))

        if self.kafka_topic is None:
            self.kafka_topic = settings.get('KAFKA_START_URLS_TOPIC')

        if not self.kafka_topic.strip():
            raise ValueError("kafka topic must not be empty")

        self.logger.info("Reading start URLs from kafka topic '%(kafka_topic)s' ", self.__dict__)

        bootstrap_servers = settings.get('KAFKA_BOOTSTRAP_SERVERS')
        kafka_params = settings.get('KAFKA_START_URLS_CONSUMER_PARAMS')
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, **kafka_params)
        self.consumer.subscribe(self.kafka_topic)
        # The idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from redis queue
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def start_requests(self):
        """Returns a batch of start requests from redis."""
        return self.next_requests()

    def next_requests(self):
        msgs = self.consumer.poll()
        partitions = self.consumer.assignment()
        for part in partitions:
            records = msgs.get(part)
            for r in (records or []):
                yield self.make_request_from_data(r.value)

    def make_request_from_data(self, data):
        """Returns a Request instance from data coming from Redis.
        By default, ``data`` is an encoded URL. You can override this method to
        provide your own message decoding.
        Parameters
        ----------
        data : bytes
            Message from redis.
        """
        raise NotImplemented

    def schedule_next_requests(self):
        """Schedules a request if available"""
        # TODO: While there is capacity, schedule a batch of redis requests.
        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        # XXX: Handle a sentinel to close the spider.
        self.schedule_next_requests()
        raise DontCloseSpider


class KafkaSpider(KafkaMixin, Spider):
    """
    # 简介

    通过继承本类， 可从 Kafka 指定 Topic 中拉取初始 url.
    作用同 scrapy_redis 一致， 只不过 redis 变成了 kafka
    """

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        obj = super(KafkaSpider, cls).from_crawler(crawler, *args, **kwargs)
        obj.setup_kafka(crawler)
        return obj
