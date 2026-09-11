--liquibase formatted sql

--changeset mentee:20260907-create-outbox-event-table
CREATE TABLE outbox_event (
                              id UUID PRIMARY KEY,
                              order_id UUID NOT NULL,
                              event_type VARCHAR(64) NOT NULL,
                              payload TEXT NOT NULL,
                              status VARCHAR(16) NOT NULL DEFAULT 'NEW',
                              attempts INTEGER NOT NULL DEFAULT 0,
                              next_retry_at TIMESTAMP WITH TIME ZONE,
                              created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                              sent_at TIMESTAMP WITH TIME ZONE,
                              last_error VARCHAR(500)
);

CREATE INDEX idx_outbox_event_dispatch ON outbox_event (status, next_retry_at, created_at);
CREATE INDEX idx_outbox_event_order_id ON outbox_event (order_id);

--rollback DROP TABLE outbox_event;