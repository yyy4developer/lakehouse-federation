CREATE TABLE IF NOT EXISTS work_orders (
    order_id    SERIAL PRIMARY KEY,
    machine_id  INTEGER NOT NULL,
    priority    VARCHAR(10) NOT NULL,
    status      VARCHAR(20) NOT NULL,
    created_date DATE NOT NULL,
    due_date    DATE NOT NULL,
    assigned_to VARCHAR(80) NOT NULL
);
