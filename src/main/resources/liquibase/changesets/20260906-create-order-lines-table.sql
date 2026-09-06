--liquibase formatted sql

--changeset mentee:20260906-create-order-lines
CREATE TABLE order_lines (
                             id         UUID PRIMARY KEY,
                             order_id   UUID NOT NULL REFERENCES orders(id),
                             product_id UUID NOT NULL,
                             quantity   INTEGER NOT NULL,
                             price      NUMERIC(19, 2) NOT NULL
);

CREATE INDEX idx_order_lines_order_id ON order_lines (order_id);

--rollback DROP TABLE order_lines;