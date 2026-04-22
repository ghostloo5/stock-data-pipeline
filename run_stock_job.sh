#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY_FILE="$SCRIPT_DIR/登鼎网络-A股数据抓取与处理流程实现.py"

cd "$SCRIPT_DIR"

if [[ ! -f "$SCRIPT_DIR/.env" && -f "$SCRIPT_DIR/.env.example" ]]; then
  cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
  echo "已创建 .env（来自 .env.example），请按需修改后重试。"
fi

echo "正在安装/更新依赖..."
python3 -m pip install -U akshare redis pymysql loguru python-dotenv pandas requests urllib3

echo "正在运行抓取任务..."
python3 "$PY_FILE"
