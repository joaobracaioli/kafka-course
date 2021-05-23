plugins {
    kotlin("jvm") version "1.5.0"
}

group = "com.kafka.suite"
version = "1.0-SNAPSHOT"



dependencies {
    implementation("org.apache.kafka:kafka-clients:2.8.0")
    implementation("org.slf4j:slf4j-simple:1.7.30")
}

repositories {
    mavenLocal()
    mavenCentral()
}
