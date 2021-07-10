package elasticsearch

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import java.io.FileInputStream
import java.time.Duration
import java.util.Collections
import java.util.Properties

fun main() {

    val properties = Properties()
    var propertiesFile = getResource("config.properties")
    properties.load(FileInputStream(propertiesFile))

    val topic = "twitter_tweets"

    val consumer = createKafkaConsumer(topic)

    val client = createClient(
        properties.getProperty("hostname"),
        properties.getProperty("username"),
        properties.getProperty("password")
    )

    val bulkRequest = BulkRequest()
    var countReceived = 0
    while (true) {
        val poll = consumer.poll(Duration.ofMillis(1000))
        countReceived = poll.count()
        println("Received $countReceived records")
        poll.forEach {

            // val idKafka =

            val idIdempotent = extractIdFromTwitter(it.value()) ?: "${it.topic()}_${it.partition()}_${it.offset()}"

            val newIndexRequest = IndexRequest(
                "twitter"
            )
                .source(it.value(), XContentType.JSON)
                .index(idIdempotent)

            bulkRequest.add(newIndexRequest) // add to our bulk request
        }

        if (countReceived > 0) {
            val buckResponse: BulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)

            Thread.sleep(10)

            println("commit")

            consumer.commitAsync()

            println("commit offset :D ")
        }
    }

    // client.close()
}

fun createClient(host: String, user: String, password: String): RestHighLevelClient {
    var clientBuilder = RestClient.builder(HttpHost(host, 443, "https"))

    clientBuilder.setHttpClientConfigCallback {
        val basicCredentialsProvider = BasicCredentialsProvider()
        basicCredentialsProvider.setCredentials(
            AuthScope.ANY,
            UsernamePasswordCredentials(user, password)
        )
        it.setDefaultCredentialsProvider(basicCredentialsProvider)
    }
    return RestHighLevelClient(clientBuilder)
}

fun createKafkaConsumer(topic: String): KafkaConsumer<String, String> {
    val bootstratapServers = "127.0.0.1:9092"
    val groupId = "group-twitter_tweets"
    val properties = Properties()

    println("Consumer !")
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstratapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // config to start
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") // disable commit offset
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")

    val consumer = KafkaConsumer<String, String>(properties)

    consumer.subscribe(Collections.singleton(topic))

    return consumer
}

fun extractIdFromTwitter(json: String): String? {
    return JsonParser.parseString(json)
        .asJsonObject
        .get("id_str")
        .asString
}

fun getResource(path: String): String {
    return Thread.currentThread().contextClassLoader.getResource(path).file
}
