import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

fun main(args: Array<String>) {

    val bootstratapServers = "127.0.0.1:9092"
    val groupId = "group-application"
    val topic = "first_topic"

    val properties = Properties()

    println("Consumer !")
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstratapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = KafkaConsumer<String, String>(properties)

    // create consumer fetch a specific message

    val topicPartition = TopicPartition(topic, 0)
    val offsetReadFrom = 10L

    consumer.assign(arrayListOf(topicPartition))
    consumer.seek(topicPartition, offsetReadFrom)

    val numberOfMessageToRead = 5
    var keepOnReading = true
    var numberOfMessageReadSoFar = 0

    while (keepOnReading) {
        val poll = consumer.poll(Duration.ofMillis(100))

        poll.forEach {
            println("key: ${it.key()}  value: ${it.value()}")
            numberOfMessageReadSoFar += 1
            if (numberOfMessageReadSoFar >= numberOfMessageToRead) {
                keepOnReading = false
                return
            }
        }
    }

}
