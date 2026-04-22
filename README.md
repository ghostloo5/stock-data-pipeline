# A股数据抓取与处理流程实现

该项目用于抓取 A 股涨停相关股票数据，完成从数据获取、缓存、持久化到查询验证的完整闭环，服务内部策略研究与分析场景。

## 功能概览

- 使用 `akshare` 抓取最近涨停股票与实时行情
- 使用 `Redis` 缓存股票最新价格与抓取时间
- 使用 `MySQL` 持久化保存历史快照
- 支持接口失败重试、数据源降级与模拟数据回退
- 提供一键脚本完成依赖安装与任务执行

## 项目结构

- `登鼎网络-A股数据抓取与处理流程实现.py`：主程序
- `run_stock_job.sh`：一键运行脚本（安装依赖 + 启动抓取）
- `setup_mysql.sql`：MySQL 建库脚本
- `.env.example`：环境变量示例

## 环境要求

- macOS / Linux
- Python 3.10+
- MySQL 8+ / 9+
- Redis 6+

## 环境变量说明

以下变量通过 `.env` 配置：

- `REDIS_HOST`：Redis 主机地址，默认 `localhost`，必填
- `REDIS_PORT`：Redis 端口，默认 `6379`，必填
- `REDIS_DB`：Redis 库编号，默认 `0`，必填
- `MYSQL_HOST`：MySQL 主机地址，默认 `localhost`，必填
- `MYSQL_USER`：MySQL 用户名，默认 `root`，必填
- `MYSQL_PASS`：MySQL 密码，可为空（取决于本机 MySQL 配置），必填
- `MYSQL_DB`：MySQL 数据库名，默认 `stock_db`，必填
- `MAX_LIMIT_UP_DAYS`：抓取涨停历史天数上限，默认 `2`，可选
- `MAX_TARGET_STOCKS`：单次最多处理股票数，默认 `20`，可选
- `MAX_BACKUP_HIST_STOCKS`：备用历史接口最大抓取数，默认 `10`，可选
- `REQUEST_RETRY_TIMES`：接口失败重试次数，默认 `3`，可选
- `REQUEST_RETRY_SLEEP`：重试间隔秒数，默认 `1.0`，可选
- `REQUEST_THROTTLE_SECONDS`：逐笔请求节流秒数，默认 `0.4`，可选
- `ALLOW_MOCK_DATA`：是否允许接口全失败时生成模拟数据，默认 `true`，可选
- `REDIS_TTL_SECONDS`：Redis 缓存过期时间（秒），默认 `86400`，可选

## 快速开始

### 1) 准备环境变量

```bash
cp .env.example .env
```

按实际环境修改 `.env` 中的数据库与缓存参数。

可直接使用以下最小配置：

```env
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASS=
MYSQL_DB=stock_db
ALLOW_MOCK_DATA=true
```

### 2) 启动 MySQL 与 Redis

```bash
brew services start mysql
brew services start redis
```

### 3) 初始化数据库（首次）

```bash
mysql -u root < setup_mysql.sql
```

### 4) 运行任务

```bash
./run_stock_job.sh
```

## 验证结果

运行成功后可在日志中看到：

- `MySQL连接成功`
- `MySQL股票表初始化完成`
- `Redis中找到...个股票缓存`
- `MySQL查询结果（最近...条记录）`

## 常见问题

- `Access denied for user 'root'@'localhost'`  
  说明 `.env` 中 `MYSQL_PASS` 与本机 MySQL 密码不一致，请统一配置。

- `Connection refused`  
  通常是 MySQL 或 Redis 服务未启动，先执行 `brew services start mysql` 与 `brew services start redis`。

- 东方财富接口偶发失败  
  程序会自动重试并降级到其他数据源。

## GitHub 提交建议

建议提交：

- 主程序、脚本、SQL、README、`.env.example`、`.gitignore`

不要提交：

- `.env`
- `stock_high_limit.log`
- `__pycache__/`
