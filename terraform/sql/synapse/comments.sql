-- shift_schedules ビューのコメント
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'シフトスケジュール - 各機械のオペレーター配置とシフト情報を管理',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='shift_schedules';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'シフトID',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='shift_schedules', @level2type=N'COLUMN', @level2name='shift_id';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'機械ID (1-10)',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='shift_schedules', @level2type=N'COLUMN', @level2name='machine_id';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'オペレーター名',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='shift_schedules', @level2type=N'COLUMN', @level2name='operator_name';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'シフト日',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='shift_schedules', @level2type=N'COLUMN', @level2name='shift_date';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'シフト種別（日勤/夜勤）',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='shift_schedules', @level2type=N'COLUMN', @level2name='shift_type';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'勤務時間（時間）',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='shift_schedules', @level2type=N'COLUMN', @level2name='hours_worked';

-- energy_consumption ビューのコメント
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'電力消費量 - 各機械の日次電力使用量とコストを記録',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='energy_consumption';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'記録ID',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='energy_consumption', @level2type=N'COLUMN', @level2name='record_id';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'機械ID (1-10)',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='energy_consumption', @level2type=N'COLUMN', @level2name='machine_id';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'測定日',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='energy_consumption', @level2type=N'COLUMN', @level2name='measure_date';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'消費電力量（kWh）',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='energy_consumption', @level2type=N'COLUMN', @level2name='kwh_consumed';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'ピーク需要（kW）',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='energy_consumption', @level2type=N'COLUMN', @level2name='peak_demand_kw';
EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'電力コスト（USD）',
  @level0type=N'SCHEMA', @level0name='dbo', @level1type=N'VIEW', @level1name='energy_consumption', @level2type=N'COLUMN', @level2name='cost_usd';
