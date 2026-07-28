-- +goose Up

-- The acknowledgement is an operational fence, not a PostgreSQL lock.  An
-- ACCESS EXCLUSIVE lock cannot undo a legacy sender that already read a
-- delivery and is about to perform its external send.  Operators must stop
-- every legacy sender first, then run the following SQL, and only then rerun
-- Goose so the destructive migration can proceed:
--
--   INSERT INTO delivery_execution_v2_cutover_ack
--       (acknowledgement_id, acknowledged_by)
--   VALUES (TRUE, current_user);
--
-- This migration creates the gate and commits it independently.  Therefore a
-- normal `goose up` deliberately stops at the destructive v2 migration until
-- the operator has made that acknowledgement.
CREATE TABLE delivery_execution_v2_cutover_ack (
    acknowledgement_id boolean PRIMARY KEY,
    acknowledged_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    acknowledged_by text NOT NULL,
    CONSTRAINT ck_delivery_execution_v2_cutover_ack_singleton
        CHECK (acknowledgement_id)
);

COMMENT ON TABLE delivery_execution_v2_cutover_ack IS
    'Insert acknowledgement_id=TRUE after stopping every legacy sender and before running the destructive delivery execution v2 cutover';

-- +goose Down
DROP TABLE delivery_execution_v2_cutover_ack;
