-- 创建数据库
CREATE DATABASE task_container_db;

-- 使用数据库
\c task_container_db;

-- 创建任务配置表
CREATE TABLE task_configs (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    task_schedule VARCHAR(255),
    task_data JSON,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建接口调用结果表
CREATE TABLE api_call_results (
    id SERIAL PRIMARY KEY,
    task_id INTEGER REFERENCES task_configs(id),
    csv_file_path VARCHAR(500),
    api_response JSON,
    parsed_result JSON,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending'
);

-- 插入示例数据
INSERT INTO task_configs (task_name, task_schedule, task_data, enabled) VALUES
('sample_task', '*/30 * * * *', '{"type": "data_sync", "source": "api_endpoint", "target": "database", "params": {"batch_size": 100}}', TRUE),
('daily_report', '0 2 * * *', '{"type": "report_generation", "report_type": "daily", "recipients": ["admin@example.com"]}', TRUE),
('batch_api_task', '0 */6 * * *', '{"type": "batch_api_call", "api_endpoint": "http://example.com/api/batch", "csv_file_path": "/data/input.csv", "result_table": "api_call_results"}', TRUE);

-- 创建更新时间触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为task_configs表创建更新时间触发器
CREATE TRIGGER update_task_configs_updated_at 
    BEFORE UPDATE ON task_configs 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();