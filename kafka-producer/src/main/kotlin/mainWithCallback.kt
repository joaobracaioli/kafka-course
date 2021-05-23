
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

fun main(args: Array<String>) {

    val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')

    val bootstratapServers = "127.0.0.1:9092"
    val properties = Properties()

    println("Producer !")
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstratapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

    val producer = KafkaProducer<String, String>(properties)

    for (i in 1..10) {
        val randomString = (1..10)
            .map { i -> kotlin.random.Random.nextInt(0, charPool.size) }
            .map(charPool::get)
            .joinToString("")

        val key = "id_$i"
        val record = ProducerRecord<String, String>("first_topic", key, randomString)

        val send = producer.send(
            record
        )

        println(" Record medatada topic ${send.get().topic()}, partition ${send.get().partition()}, offset ${send.get().offset()}}")
    }

    producer.flush()
    producer.close()
}
