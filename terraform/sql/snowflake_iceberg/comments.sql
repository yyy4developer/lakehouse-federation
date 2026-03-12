-- Iceberg tables use ALTER ICEBERG TABLE (not COMMENT ON TABLE)
ALTER ICEBERG TABLE operational_metrics SET COMMENT = 'OEE運転メトリクス - 各製造機器の稼働率・生産性・品質スコア（machine_id 1-10、他ソースとJOIN可能）';
ALTER ICEBERG TABLE operational_metrics ALTER COLUMN metric_id COMMENT 'メトリクスレコードの一意識別子';
ALTER ICEBERG TABLE operational_metrics ALTER COLUMN machine_id COMMENT '機器ID（1-10、他ソースとJOIN可能）';
ALTER ICEBERG TABLE operational_metrics ALTER COLUMN metric_date COMMENT '計測日';
ALTER ICEBERG TABLE operational_metrics ALTER COLUMN oee_score COMMENT '総合設備効率（OEE）スコア';
ALTER ICEBERG TABLE operational_metrics ALTER COLUMN availability_pct COMMENT '可動率';
ALTER ICEBERG TABLE operational_metrics ALTER COLUMN performance_pct COMMENT '性能率';
ALTER ICEBERG TABLE operational_metrics ALTER COLUMN quality_pct COMMENT '良品率';

ALTER ICEBERG TABLE safety_incidents SET COMMENT = '安全インシデント記録 - 各機器の安全事象（深刻度、説明、是正措置、解決状況）';
ALTER ICEBERG TABLE safety_incidents ALTER COLUMN incident_id COMMENT 'インシデントレコードの一意識別子';
ALTER ICEBERG TABLE safety_incidents ALTER COLUMN machine_id COMMENT '対象機器ID（1-10、他ソースとJOIN可能）';
ALTER ICEBERG TABLE safety_incidents ALTER COLUMN incident_date COMMENT '発生日';
ALTER ICEBERG TABLE safety_incidents ALTER COLUMN severity COMMENT '深刻度（HIGH/MEDIUM/LOW）';
ALTER ICEBERG TABLE safety_incidents ALTER COLUMN description COMMENT 'インシデントの説明';
ALTER ICEBERG TABLE safety_incidents ALTER COLUMN corrective_action COMMENT '是正措置';
ALTER ICEBERG TABLE safety_incidents ALTER COLUMN resolved COMMENT '解決済みフラグ'
