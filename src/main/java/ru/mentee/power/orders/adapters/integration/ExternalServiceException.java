package ru.mentee.power.orders.adapters.integration;

/**
 * Помечает временный сбой внешнего сервиса (сеть, таймаут, 5xx) —
 * единственный тип, который Resilience4j повторяет (см. retry-exceptions в application.yml).
 */
public class ExternalServiceException extends RuntimeException{
    public ExternalServiceException(String message,  Throwable cause) {
        super(message, cause);
    }
}
