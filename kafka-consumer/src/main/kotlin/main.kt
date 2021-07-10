import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Collections
import java.util.Properties

fun main(args: Array<String>) {

    val bootstratapServers = "127.0.0.1:9092"
    val groupId = "group-application-12"
    val topic = "first_topic"

    val properties = Properties()

    println("Consumer !")
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstratapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // config to start

    val consumer = KafkaConsumer<String, String>(properties)

    consumer.subscribe(Collections.singleton(topic))

    while (true) {
        val poll = consumer.poll(Duration.ofMillis(100))

        poll.forEach {
            println("key: ${it.key()}  value: ${it.value()}")
        }
    }
}
