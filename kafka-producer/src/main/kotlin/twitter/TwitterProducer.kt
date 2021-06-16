package twitter

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.http.HttpHost
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.util.Properties
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

private val logger: Logger by lazy { LoggerFactory.getLogger("Twitter") }



fun main() {

    logger.info("Start application")

    val properties = Properties()

    var propertiesFile = getResource("config.properties")
    properties.load(FileInputStream(propertiesFile))

    val msgQueue = LinkedBlockingQueue<String>(1)

    // create twitter client
    val client = twitterClientFactory(msgQueue, properties)
    client.connect()

    // create kafka producert
    val producer = createKafkaProducer()

    Runtime.getRuntime().addShutdownHook(
        Thread {
            logger.info("stopping application...")
            logger.info("shutting down client from twitter...")
            client.stop()
            logger.info("closing producer...")
            producer.close()
            logger.info("done!")
        }
    )

    // send tweets to kafka
    sendTweeterToKafka(client, msgQueue, producer)

    logger.info("End application")
}

fun getResource(path: String): String {
    return Thread.currentThread().contextClassLoader.getResource(path).file
}

fun createKafkaProducer(): KafkaProducer<String, String> {
    val bootstratapServers = "127.0.0.1:9092"
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstratapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

    return KafkaProducer<String, String>(properties)
}

fun sendTweeterToKafka(client: Client, msgQueue: LinkedBlockingQueue<String>, producer: KafkaProducer<String, String>) {
    while (!client.isDone) {
        val msg = msgQueue.poll(5, TimeUnit.SECONDS)

        val callback = Callback { metadata, e ->
            if (e != null) {
                logger.error("Something bad happened", e)
            }
        }
        msg.let {
            logger.info(msg)
            producer.send(producerRecords(msg), callback)
        }
    }
}

fun producerRecords(msg: String): ProducerRecord<String, String> {
    val i = UUID.randomUUID()
    return ProducerRecord<String, String>("twitter_tweets", "id_$i", msg)
}

fun twitterClientFactory(msgQueue: LinkedBlockingQueue<String>, properties: Properties): BasicClient {

    val hosts = HttpHost(Constants.STREAM_HOST)
    val hostEndpoint = StatusesFilterEndpoint()

    val terms = listOf("kafka", "bitcoin", "java")

    hostEndpoint.trackTerms(terms)
    properties.getProperty("consumerKey")
    val auth = OAuth1(properties.getProperty("consumerKey"),
        properties.getProperty("consumerSecret"),
        properties.getProperty("token"),
        properties.getProperty("secret"))

    return ClientBuilder()
        .name("client-1")
        .hosts(hosts.hostName)
        .endpoint(hostEndpoint)
        .authentication(auth)
        .processor(StringDelimitedProcessor(msgQueue))
        .build()
}
