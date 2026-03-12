CREATE TABLE IF NOT EXISTS spare_parts_inventory (
    part_id          INT          NOT NULL,
    machine_id       INT          NOT NULL,
    part_name        VARCHAR(60)  NOT NULL,
    part_number      VARCHAR(30)  NOT NULL,
    quantity_on_hand INT          NOT NULL,
    reorder_point    INT          NOT NULL,
    unit_cost_usd    DECIMAL(10,2),
    last_ordered     DATE,
    PRIMARY KEY (part_id)
);
