--liquibase formatted sql

--changeset mentee:20260912-create-saga-execution-log-table
CREATE TABLE saga_execution_log (
                                    saga_id       UUID NOT NULL,
                                    step_name     VARCHAR(64) NOT NULL,
                                    order_id      UUID NOT NULL,
                                    status        VARCHAR(32) NOT NULL,
                                    error_message TEXT,
                                    created_at    TIMESTAMP WITH TIME ZONE NOT NULL,
                                    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL,
                                    PRIMARY KEY (saga_id, step_name)
);

CREATE INDEX idx_saga_execution_log_order_id ON saga_execution_log (order_id);

--rollback DROP TABLE saga_execution_log;