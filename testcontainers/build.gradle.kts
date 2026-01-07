plugins {
    java
    id("org.jetbrains.kotlin.jvm") version "1.9.20"
}

group = "com.doordash.spark"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // TestContainers
    implementation("org.testcontainers:testcontainers:1.19.3")
    implementation("org.testcontainers:junit-jupiter:1.19.3")

    // Spark Connect Client
    implementation("org.apache.spark:spark-connect-client-jvm_2.12:3.5.0")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.mockito:mockito-core:5.7.0")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.4.11")
}

tasks.test {
    useJUnitPlatform()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}
