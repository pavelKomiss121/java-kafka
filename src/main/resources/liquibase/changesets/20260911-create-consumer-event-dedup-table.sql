--liquibase formatted sql

--changeset mentee:20260911-create-consumer-event-dedup-table
CREATE TABLE consumer_event_dedup (
                                      order_id     UUID NOT NULL,
                                      event_id     UUID NOT NULL,
                                      processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                                      expires_at   TIMESTAMP WITH TIME ZONE NOT NULL,
                                      PRIMARY KEY (order_id, event_id)
);

CREATE INDEX idx_consumer_event_dedup_expires_at ON consumer_event_dedup (expires_at);

--rollback DROP TABLE consumer_event_dedup;