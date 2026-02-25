"""列出多维表格中的所有数据表"""

import sys
from pathlib import Path

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.bitable_helper import get_tenant_access_token
from scripts.feishu_helper import load_feishu_config
import requests

def list_tables(app_token: str, access_token: str):
    """列出多维表格中的所有数据表"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    result = response.json()

    if result.get("code") != 0:
        print(f"❌ 获取数据表列表失败: {result}")
        return

    tables = result["data"]["items"]
    print(f"\n找到 {len(tables)} 个数据表：")
    print("=" * 60)
    for table in tables:
        print(f"表名: {table['name']}")
        print(f"table_id: {table['table_id']}")
        print("-" * 60)

if __name__ == "__main__":
    APP_TOKEN = "Q0PTbRjuIacNt8sOoVFcXyOdn1c"

    config = load_feishu_config()
    access_token = get_tenant_access_token(config["app_id"], config["app_secret"])

    list_tables(APP_TOKEN, access_token)
