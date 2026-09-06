--liquibase formatted sql

--changeset mentee:20260906-add-consumer-columns
ALTER TABLE orders ADD COLUMN region VARCHAR(64);
ALTER TABLE orders ADD COLUMN priority VARCHAR(16);
ALTER TABLE orders ADD COLUMN kafka_partition INTEGER;
ALTER TABLE orders ADD COLUMN kafka_offset BIGINT;
ALTER TABLE orders ADD COLUMN processed_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX idx_orders_region ON orders (region);
CREATE INDEX idx_orders_priority ON orders (priority);

--rollback ALTER TABLE orders DROP COLUMN region;
--rollback ALTER TABLE orders DROP COLUMN priority;
--rollback ALTER TABLE orders DROP COLUMN kafka_partition;
--rollback ALTER TABLE orders DROP COLUMN kafka_offset;
--rollback ALTER TABLE orders DROP COLUMN processed_at;