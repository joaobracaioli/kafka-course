import com.google.gson.JsonParser
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.message.MessageFactory
import java.util.Properties

private val logger = LogManager.getLogger()

private val messageFactory = logger.getMessageFactory<MessageFactory>()

fun main() {
    val bootstratapServers = "127.0.0.1:9092"
    val properties = Properties()
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstratapServers)
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams")
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java.name)
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java.name)

    val streamBuilder = StreamsBuilder()

    val inputTopic = streamBuilder.stream<String, String>("twitter_tweets")

    val filterStreamTopic = inputTopic.filter { _, json -> extractUserFollowers(json) > 10000 }
    filterStreamTopic.to("important_tweets")

    val kafkaStreams = KafkaStreams(streamBuilder.build(), properties)

    kafkaStreams.start()
}

fun extractUserFollowers(twitter: String): Int {
    return JsonParser.parseString(twitter)
        .asJsonObject
        .get("user")
        .asJsonObject
        .get("followers_count")
        .asInt
}
