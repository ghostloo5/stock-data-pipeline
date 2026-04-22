CREATE DATABASE IF NOT EXISTS stock_db
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

-- 如需独立账号，可取消注释并修改密码
-- CREATE USER IF NOT EXISTS 'stock_user'@'localhost' IDENTIFIED BY 'ChangeMe_123!';
-- GRANT ALL PRIVILEGES ON stock_db.* TO 'stock_user'@'localhost';
-- FLUSH PRIVILEGES;
