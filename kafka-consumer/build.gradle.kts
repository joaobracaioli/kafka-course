import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.0"
    application
}

group = "com.kafka.suite"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:2.8.0")
    implementation("org.slf4j:slf4j-simple:1.7.30")
    implementation("org.elasticsearch.client:elasticsearch-rest-client:6.2.3")
    implementation("org.elasticsearch.client:elasticsearch-rest-high-level-client:7.13.2")

    implementation("com.google.code.gson:gson:2.8.6")

}

repositories {
    mavenLocal()
    mavenCentral()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}

application {
    mainClassName = "MainKt"
}
