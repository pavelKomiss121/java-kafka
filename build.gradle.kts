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

    // Resilience4j: управляемый retry + AOP-прокси для @Retry
    implementation("io.github.resilience4j:resilience4j-spring-boot3:2.2.0")
    implementation("org.springframework.boot:spring-boot-starter-aop")

    // Тесты
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    // для профиля ci — in-memory БД
    testRuntimeOnly("com.h2database:h2")
    runtimeOnly("com.h2database:h2")
    // Тесты: мок внешнего HTTP-сервиса для проверки retry/fallback
    testImplementation("org.wiremock:wiremock-standalone:3.9.2")
    // Метрики outbox: Micrometer + Prometheus registry, /actuator/prometheus
    implementation("io.micrometer:micrometer-registry-prometheus")
}

tasks.withType<Test> {
    useJUnitPlatform()
}