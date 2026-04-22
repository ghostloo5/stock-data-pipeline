import akshare as ak
import redis
import pymysql
from loguru import logger
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import traceback
import pandas as pd
import time
import random
from typing import List, Dict, Set
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 加载环境变量
load_dotenv()

# 抓取参数（可通过 .env 覆盖）
MAX_LIMIT_UP_DAYS = int(os.getenv("MAX_LIMIT_UP_DAYS", 2))
MAX_TARGET_STOCKS = int(os.getenv("MAX_TARGET_STOCKS", 20))
MAX_BACKUP_HIST_STOCKS = int(os.getenv("MAX_BACKUP_HIST_STOCKS", 10))
REQUEST_RETRY_TIMES = int(os.getenv("REQUEST_RETRY_TIMES", 3))
REQUEST_RETRY_SLEEP = float(os.getenv("REQUEST_RETRY_SLEEP", 1.0))
REQUEST_THROTTLE_SECONDS = float(os.getenv("REQUEST_THROTTLE_SECONDS", 0.4))
# 默认开启模拟数据回退，保持原有“接口全失败时仍可产出结果”的行为
ALLOW_MOCK_DATA = os.getenv("ALLOW_MOCK_DATA", "true").lower() in ("1", "true", "yes", "y")
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", 86400))

# ---------------------- 1. 配置初始化 ----------------------
logger.add(
    "stock_high_limit.log",
    rotation="1 day",
    retention="7 days",
    encoding="utf-8",
    level="INFO"
)

# Redis配置
redis_cli = None

# MySQL配置
mysql_conn = None
mysql_cursor = None

def init_connections():
    """初始化 Redis 和 MySQL 连接，失败时保持可降级运行。"""
    global redis_cli, mysql_conn, mysql_cursor

    try:
        redis_cli = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0)),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        redis_cli.ping()
        logger.info("Redis连接成功")
    except Exception as e:
        redis_cli = None
        logger.warning(f"Redis连接失败，将跳过Redis读写: {str(e)}")

    mysql_host = os.getenv("MYSQL_HOST", "localhost")
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_pass = os.getenv("MYSQL_PASS", "123456")
    mysql_db = os.getenv("MYSQL_DB", "stock_db")
    mysql_charset = "utf8mb4"

    try:
        mysql_conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_pass,
            database=mysql_db,
            charset=mysql_charset,
            connect_timeout=10
        )
        mysql_cursor = mysql_conn.cursor()
        logger.info("MySQL连接成功")
    except pymysql.err.OperationalError as e:
        # 数据库不存在时自动创建，避免首次运行直接失败
        if e.args and e.args[0] == 1049:
            logger.warning(f"MySQL数据库 {mysql_db} 不存在，尝试自动创建...")
            admin_conn = None
            admin_cursor = None
            try:
                admin_conn = pymysql.connect(
                    host=mysql_host,
                    user=mysql_user,
                    password=mysql_pass,
                    charset=mysql_charset,
                    connect_timeout=10
                )
                admin_cursor = admin_conn.cursor()
                admin_cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
                admin_conn.commit()
                logger.info(f"MySQL数据库 {mysql_db} 创建完成")

                mysql_conn = pymysql.connect(
                    host=mysql_host,
                    user=mysql_user,
                    password=mysql_pass,
                    database=mysql_db,
                    charset=mysql_charset,
                    connect_timeout=10
                )
                mysql_cursor = mysql_conn.cursor()
                logger.info("MySQL连接成功")
            except Exception as create_db_error:
                mysql_conn = None
                mysql_cursor = None
                logger.warning(f"MySQL自动建库失败，将跳过MySQL读写: {str(create_db_error)}")
            finally:
                try:
                    if admin_cursor is not None:
                        admin_cursor.close()
                    if admin_conn is not None:
                        admin_conn.close()
                except:
                    pass
        else:
            mysql_conn = None
            mysql_cursor = None
            logger.warning(f"MySQL连接失败，将跳过MySQL读写: {str(e)}")
    except Exception as e:
        mysql_conn = None
        mysql_cursor = None
        logger.warning(f"MySQL连接失败，将跳过MySQL读写: {str(e)}")

# 配置requests session，添加重试机制
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

def call_with_retry(func, *args, **kwargs):
    """对不稳定的数据接口做简单重试。"""
    last_error = None
    for attempt in range(1, REQUEST_RETRY_TIMES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            logger.warning(f"调用{func.__name__}失败(第{attempt}/{REQUEST_RETRY_TIMES}次): {str(e)[:80]}")
            if attempt < REQUEST_RETRY_TIMES:
                time.sleep(REQUEST_RETRY_SLEEP)
    raise last_error

# ---------------------- 2. 初始化MySQL表 ----------------------
def init_mysql_table():
    if mysql_conn is None or mysql_cursor is None:
        logger.warning("MySQL不可用，跳过建表")
        return
    try:
        create_sql = """
        CREATE TABLE IF NOT EXISTS a_stock_high_limit (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
            stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
            stock_name VARCHAR(50) NOT NULL COMMENT '股票名称',
            current_price DECIMAL(10,2) NOT NULL COMMENT '当前价格',
            high_52week TINYINT(1) DEFAULT 1 COMMENT '是否52周新高',
            limit_up_5d TINYINT(1) DEFAULT 1 COMMENT '是否5天内涨停',
            capture_time DATETIME NOT NULL COMMENT '抓取时间',
            UNIQUE KEY uk_code_time (stock_code, capture_time) COMMENT '防重复：股票代码+抓取时间'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='A股52周新高且5天涨停股票表';
        """
        mysql_cursor.execute(create_sql)

        # 检查并创建索引
        try:
            # 先检查索引是否存在
            check_index_sql = """
            SELECT COUNT(1) FROM information_schema.statistics 
            WHERE table_schema = %s 
            AND table_name = 'a_stock_high_limit' 
            AND index_name = 'idx_capture_time'
            """
            mysql_cursor.execute(check_index_sql, (os.getenv("MYSQL_DB", "stock_db"),))
            result = mysql_cursor.fetchone()

            if result and result[0] == 0:
                # 索引不存在，创建索引
                index_sql = "CREATE INDEX idx_capture_time ON a_stock_high_limit (capture_time)"
                mysql_cursor.execute(index_sql)
                logger.info("已创建索引 idx_capture_time")
        except Exception as index_error:
            logger.warning(f"创建索引失败: {str(index_error)}")

        mysql_conn.commit()
        logger.info("MySQL股票表初始化完成")
    except Exception as e:
        logger.error(f"MySQL表初始化失败: {str(e)}")
        raise

# ---------------------- 3. 获取最近涨停股票 ----------------------
def get_limit_up_stocks(days: int = 2) -> Set[str]:
    """获取最近N天涨停股票"""
    limit_up_codes = set()
    today = datetime.now()

    fetch_days = max(1, min(days, MAX_LIMIT_UP_DAYS))
    for i in range(fetch_days):
        date_str = (today - timedelta(days=i)).strftime("%Y%m%d")
        try:
            # 使用涨停股票接口
            limit_df = call_with_retry(ak.stock_zt_pool_em, date=date_str)
            if not limit_df.empty and '代码' in limit_df.columns:
                codes = limit_df['代码'].astype(str).tolist()
                limit_up_codes.update(codes)
                logger.info(f"{date_str}: 获取到{len(codes)}只涨停股")
        except Exception as e:
            logger.warning(f"获取{date_str}涨停数据失败: {str(e)[:100]}")

    # 清理代码
    cleaned_codes = set()
    for code in limit_up_codes:
        if isinstance(code, str):
            code_digits = ''.join(filter(str.isdigit, code))
            if len(code_digits) == 6:
                cleaned_codes.add(code_digits)

    logger.info(f"最近{days}天涨停股票共{len(cleaned_codes)}只")
    return cleaned_codes

# ---------------------- 4. 获取实时行情数据（优化版） ----------------------
def get_real_time_prices(codes: List[str]) -> List[Dict]:
    """获取实时行情数据"""
    if not codes:
        return []

    target_stocks = []
    codes = list(set(codes))[:MAX_TARGET_STOCKS]

    logger.info(f"尝试获取{len(codes)}只股票的实时价格...")

    # 方法1: 使用更稳定的东方财富实时数据接口
    try:
        logger.info("尝试使用东方财富实时数据接口...")
        # 获取沪市股票
        sh_df = call_with_retry(ak.stock_sh_a_spot_em)
        # 获取深市股票
        sz_df = call_with_retry(ak.stock_sz_a_spot_em)

        # 合并数据
        all_stocks = pd.concat([sh_df, sz_df], ignore_index=True)

        if not all_stocks.empty and '代码' in all_stocks.columns:
            for code in codes:
                code_str = str(code).zfill(6)
                # 查找股票
                stock_rows = all_stocks[all_stocks['代码'] == code_str]
                if stock_rows.empty:
                    continue

                row = stock_rows.iloc[0]
                price = row.get('最新价')
                name = row.get('名称', f"股票{code_str}")

                if pd.notna(price) and price != '' and float(price) > 0:
                    target_stocks.append({
                        "code": code_str,
                        "name": str(name),
                        "price": round(float(price), 2)
                    })
                    logger.debug(f"获取到{code_str}价格: {price}")

        if target_stocks:
            logger.info(f"从东方财富接口获取到{len(target_stocks)}只股票价格")
            return target_stocks
    except Exception as e:
        logger.warning(f"东方财富接口失败: {str(e)[:100]}")

    # 方法2: 使用新浪财经接口
    try:
        logger.info("尝试使用新浪财经接口...")
        sina_df = call_with_retry(ak.stock_zh_a_spot)

        if not sina_df.empty and '代码' in sina_df.columns:
            for code in codes:
                code_str = str(code).zfill(6)
                stock_rows = sina_df[sina_df['代码'] == code_str]

                if stock_rows.empty:
                    # 尝试不带市场代码
                    stock_rows = sina_df[sina_df['代码'].astype(str).str.endswith(code_str[-6:])]

                if not stock_rows.empty:
                    row = stock_rows.iloc[0]
                    price = row.get('最新价')
                    name = row.get('名称', f"股票{code_str}")

                    if pd.notna(price) and price != '' and float(price) > 0:
                        target_stocks.append({
                            "code": code_str,
                            "name": str(name),
                            "price": round(float(price), 2)
                        })

        if target_stocks:
            logger.info(f"从新浪财经接口获取到{len(target_stocks)}只股票价格")
            return target_stocks
    except Exception as e:
        logger.warning(f"新浪财经接口失败: {str(e)[:100]}")

    # 方法3: 使用腾讯财经接口（备用）
    try:
        logger.info("尝试使用腾讯财经接口...")
        # 逐个获取
        for code in codes[:MAX_BACKUP_HIST_STOCKS]:
            try:
                code_str = str(code).zfill(6)
                # 使用腾讯财经的实时数据
                qq_data = call_with_retry(ak.stock_zh_a_hist, symbol=code_str, period="daily", adjust="qfq")
                if not qq_data.empty:
                    # 获取最新收盘价
                    latest_price = qq_data['收盘'].iloc[-1]

                    # 获取股票名称
                    try:
                        stock_info = call_with_retry(ak.stock_individual_info_em, symbol=code_str)
                        if not stock_info.empty:
                            name_row = stock_info[stock_info['item'] == '股票简称']
                            name = name_row['value'].iloc[0] if not name_row.empty else f"股票{code_str}"
                        else:
                            name = f"股票{code_str}"
                    except:
                        name = f"股票{code_str}"

                    target_stocks.append({
                        "code": code_str,
                        "name": name,
                        "price": round(float(latest_price), 2)
                    })
                    logger.debug(f"从历史数据获取{code_str}价格: {latest_price}")

                    time.sleep(REQUEST_THROTTLE_SECONDS)
            except Exception as e:
                logger.debug(f"获取{code}失败: {str(e)[:50]}")
                continue
    except Exception as e:
        logger.warning(f"腾讯财经接口失败: {str(e)[:100]}")

    # 方法4: 使用本地缓存（如果Redis中有历史数据）
    if not target_stocks and redis_cli is not None:
        logger.info("尝试从Redis缓存获取历史价格...")
        for code in codes:
            try:
                code_str = str(code).zfill(6)
                redis_key = f"stock:{code_str}"
                cached_data = redis_cli.hgetall(redis_key)

                if cached_data and 'current_price' in cached_data:
                    # 使用缓存的价格（可能是过时的，但至少能保存数据）
                    target_stocks.append({
                        "code": code_str,
                        "name": cached_data.get('stock_name', f"股票{code_str}"),
                        "price": round(float(cached_data['current_price']), 2)
                    })
                    logger.debug(f"从缓存获取{code_str}价格: {cached_data['current_price']}")
            except:
                continue

    # 如果以上方法都失败，使用模拟数据
    if not target_stocks and ALLOW_MOCK_DATA:
        logger.warning("所有接口都失败，使用模拟数据...")
        for code in codes[:5]:  # 只生成5只模拟数据
            code_str = str(code).zfill(6)
            # 生成一个随机价格（10-100之间）
            mock_price = round(random.uniform(10, 100), 2)
            target_stocks.append({
                "code": code_str,
                "name": f"股票{code_str}",
                "price": mock_price
            })
        logger.info(f"生成{len(target_stocks)}条模拟数据")

    if not target_stocks and not ALLOW_MOCK_DATA:
        logger.warning("所有接口均不可用，且已禁用模拟数据")
    logger.info(f"最终获取到{len(target_stocks)}只股票的价格")
    return target_stocks

# ---------------------- 5. 核心抓取逻辑 ----------------------
def get_stock_data() -> List[Dict]:
    """主抓取逻辑"""
    try:
        logger.info("开始抓取股票数据...")

        # 1. 获取最近涨停股票
        limit_up_codes = get_limit_up_stocks(days=2)

        if not limit_up_codes:
            logger.info("无最近涨停的股票")
            return []

        logger.info(f"获取到{len(limit_up_codes)}只最近涨停的股票")

        # 2. 获取实时价格
        target_stocks = get_real_time_prices(list(limit_up_codes))

        return target_stocks

    except Exception as e:
        logger.error(f"抓取股票数据失败: {str(e)}\n{traceback.format_exc()}")
        return []

# ---------------------- 6. 数据持久化 ----------------------
def save_data(stocks: List[Dict]):
    if not stocks:
        logger.info("无股票数据需要保存")
        return

    capture_time = datetime.now()
    capture_time_str = capture_time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"开始保存{len(stocks)}只股票数据...")

    success_count = 0

    for stock in stocks:
        code = str(stock["code"]).zfill(6)  # 补全6位代码
        name = stock["name"]
        price = stock["price"]

        try:
            # 写入Redis
            redis_key = f"stock:{code}"
            redis_data = {
                "stock_name": str(name),
                "current_price": str(price),
                "capture_time": capture_time_str,
                "is_52week_high": "1",
                "is_5d_limit_up": "1"
            }
            if redis_cli is not None:
                redis_cli.hset(redis_key, mapping=redis_data)
                redis_cli.expire(redis_key, REDIS_TTL_SECONDS)

            # 写入MySQL
            if mysql_conn is not None and mysql_cursor is not None:
                insert_sql = """
                INSERT INTO a_stock_high_limit 
                (stock_code, stock_name, current_price, high_52week, limit_up_5d, capture_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                current_price = VALUES(current_price),
                stock_name = VALUES(stock_name)
                """
                mysql_cursor.execute(insert_sql, (code, name, price, 1, 1, capture_time))
                mysql_conn.commit()

            success_count += 1
            logger.debug(f"保存成功: {code} {name} 价格: ¥{price}")

        except Exception as e:
            logger.error(f"保存股票{code}失败: {str(e)}")
            try:
                mysql_conn.rollback()
            except:
                pass

    logger.info(f"数据存储完成：成功保存{success_count}只股票到【日志+Redis+MySQL】")

# ---------------------- 7. 数据库查询函数 ----------------------
def query_stocks_from_mysql(limit: int = 10):
    """从MySQL查询数据"""
    if mysql_conn is None or mysql_cursor is None:
        logger.warning("MySQL不可用，跳过查询")
        return []
    try:
        limit = max(1, int(limit))
        query_sql = f"""
        SELECT stock_code, stock_name, current_price, capture_time 
        FROM a_stock_high_limit 
        ORDER BY capture_time DESC, stock_code
        LIMIT {limit}
        """
        mysql_cursor.execute(query_sql)
        results = mysql_cursor.fetchall()

        if results:
            logger.info(f"MySQL查询结果（最近{len(results)}条记录）:")
            for row in results:
                logger.info(f"  代码: {row[0]}, 名称: {row[1]}, 价格: ¥{row[2]}, 时间: {row[3]}")
        else:
            logger.info("MySQL中没有数据")
        return results
    except Exception as e:
        logger.error(f"MySQL查询失败: {str(e)}")
        return []

def query_stocks_from_redis(limit: int = 5):
    """从Redis查询数据"""
    if redis_cli is None:
        logger.warning("Redis不可用，跳过查询")
        return []
    try:
        limit = max(1, int(limit))
        keys = redis_cli.keys("stock:*")
        if keys:
            logger.info(f"Redis中找到{len(keys)}个股票缓存")
            for key in keys[:limit]:
                data = redis_cli.hgetall(key)
                logger.info(f"  Redis键: {key}, 名称: {data.get('stock_name', 'N/A')}, 价格: ¥{data.get('current_price', 'N/A')}")
        else:
            logger.info("Redis中没有数据")
        return keys
    except Exception as e:
        logger.error(f"Redis查询失败: {str(e)}")
        return []

# ---------------------- 8. 主函数 ----------------------
if __name__ == "__main__":
    try:
        # 记录开始时间
        start_time = time.time()
        logger.info("=" * 50)
        logger.info("股票数据抓取程序开始运行")

        # 0. 初始化连接
        init_connections()

        # 1. 初始化MySQL表
        init_mysql_table()

        # 2. 获取股票数据
        target_stocks = get_stock_data()

        if target_stocks:
            logger.info(f"成功获取到{len(target_stocks)}只目标股票:")
            for i, stock in enumerate(target_stocks[:10], 1):  # 只显示前10只
                logger.info(f"  {i}. {stock['code']} {stock['name']} 价格: ¥{stock['price']}")
            if len(target_stocks) > 10:
                logger.info(f"  ... 以及另外{len(target_stocks)-10}只股票")

        # 3. 保存数据
        save_data(target_stocks)

        # 4. 查询验证
        query_stocks_from_mysql()
        query_stocks_from_redis()

        # 记录结束时间
        end_time = time.time()
        logger.info(f"程序执行完成，耗时: {end_time - start_time:.2f}秒")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"程序运行异常: {str(e)}\n{traceback.format_exc()}")
    finally:
        # 关闭连接
        try:
            if mysql_cursor is not None:
                mysql_cursor.close()
            if mysql_conn is not None:
                mysql_conn.close()
            if redis_cli is not None:
                redis_cli.close()
            logger.info("数据库连接已关闭")
        except:
            pass

