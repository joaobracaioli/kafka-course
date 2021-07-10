plugins {
    kotlin("jvm") version "1.5.0"
}

group = "com.kafka.suite"
version = "1.0-SNAPSHOT"

dependencies {
    implementation("org.apache.kafka:kafka-clients:2.8.0")
    implementation("org.slf4j:slf4j-simple:1.7.30")
    implementation("com.twitter:hbc-core:2.2.0")
    implementation("org.apache.logging.log4j:log4j-core:2.14.1")
}

repositories {
    mavenLocal()
    mavenCentral()
}
