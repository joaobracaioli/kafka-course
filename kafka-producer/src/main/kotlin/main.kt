import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

fun main(args: Array<String>) {

    val charPool : List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')

    val bootstratapServers = "127.0.0.1:9092"
    val properties = Properties()

    println("Producer !")
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstratapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

    val randomString = (1..10)
        .map { i -> kotlin.random.Random.nextInt(0, charPool.size) }
        .map(charPool::get)
        .joinToString("");

    val record = ProducerRecord<String, String>("first_topic", randomString)

    val producer = KafkaProducer<String, String>(properties)

    producer.send(record)
    producer.flush()
    producer.close()
}
