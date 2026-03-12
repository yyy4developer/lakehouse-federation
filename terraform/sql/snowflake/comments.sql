-- equipment_specs table comments
COMMENT ON TABLE equipment_specs IS '機器仕様マスタ - 各製造機器の技術仕様（メーカー、型番、最大回転数、重量、出力）';
COMMENT ON COLUMN equipment_specs.spec_id IS '仕様レコードの一意識別子';
COMMENT ON COLUMN equipment_specs.machine_id IS '機器ID（1-10、他ソースとJOIN可能）';
COMMENT ON COLUMN equipment_specs.manufacturer IS '製造メーカー名';
COMMENT ON COLUMN equipment_specs.model_number IS '型番';
COMMENT ON COLUMN equipment_specs.max_rpm IS '最大回転数（RPM）';
COMMENT ON COLUMN equipment_specs.weight_kg IS '機器重量（kg）';
COMMENT ON COLUMN equipment_specs.power_kw IS '定格出力（kW）';
COMMENT ON COLUMN equipment_specs.install_year IS '設置年';

-- spare_parts_inventory table comments
COMMENT ON TABLE spare_parts_inventory IS '部品在庫 - 各機器の交換部品在庫状況（品名、在庫数、発注点、単価）';
COMMENT ON COLUMN spare_parts_inventory.part_id IS '部品レコードの一意識別子';
COMMENT ON COLUMN spare_parts_inventory.machine_id IS '対象機器ID（1-10、他ソースとJOIN可能）';
COMMENT ON COLUMN spare_parts_inventory.part_name IS '部品名';
COMMENT ON COLUMN spare_parts_inventory.part_number IS '部品番号';
COMMENT ON COLUMN spare_parts_inventory.quantity_on_hand IS '現在在庫数';
COMMENT ON COLUMN spare_parts_inventory.reorder_point IS '発注点（この数量以下で要発注）';
COMMENT ON COLUMN spare_parts_inventory.unit_cost_usd IS '単価（USD）';
COMMENT ON COLUMN spare_parts_inventory.last_ordered IS '最終発注日';
