IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'shift_schedules')
CREATE TABLE shift_schedules (
    shift_id     INT PRIMARY KEY,
    machine_id   INT NOT NULL,
    operator_name NVARCHAR(80) NOT NULL,
    shift_date   DATE NOT NULL,
    shift_type   NVARCHAR(20) NOT NULL,
    hours_worked DECIMAL(4,1) NOT NULL
);
