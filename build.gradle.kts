plugins {
    java
    id("org.springframework.boot") version "3.2.5"
    id("io.spring.dependency-management") version "1.1.5"
}

group = "ru.mentee.power"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // HTTP API
    implementation("org.springframework.boot:spring-boot-starter-web")

    // Kafka client (Template, AdminClient, NewTopic)
    implementation("org.springframework.kafka:spring-kafka")

    // /actuator/health, info, probes
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // БД + репозитории
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    runtimeOnly("org.postgresql:postgresql:42.7.3")

    // Миграции схемы
    implementation("org.liquibase:liquibase-core")

    // Тесты
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    // для профиля ci — in-memory БД
    testRuntimeOnly("com.h2database:h2")
    runtimeOnly("com.h2database:h2")
}

tasks.withType<Test> {
    useJUnitPlatform()
}