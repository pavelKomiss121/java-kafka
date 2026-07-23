--liquibase formatted sql

--changeset mentee:20250220-create-orders
CREATE TABLE orders (
                        id          UUID PRIMARY KEY,
                        customer_id UUID NOT NULL,
                        amount      NUMERIC(19, 2) NOT NULL,
                        status      VARCHAR(32) NOT NULL,
                        created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
                        updated_at  TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_orders_status ON orders (status);
CREATE INDEX idx_orders_created_at ON orders (created_at);

--rollback DROP TABLE orders;