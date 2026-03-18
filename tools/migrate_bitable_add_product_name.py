"""迁移脚本：给现有飞书多维表格添加"品名"字段，并迁移 unique_key

1. 给每张表添加"品名"文本字段
2. 遍历所有记录，将 unique_key 从 "日期|国家" 改为 "日期|国家|品名"，并填入品名值

用法:
    python tools/migrate_bitable_add_product_name.py              # 执行迁移
    python tools/migrate_bitable_add_product_name.py --dry-run    # 预览不执行
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from scripts.bitable_helper import get_tenant_access_token, add_field
from scripts.feishu_helper import load_feishu_config
from scripts.logger import setup_logger

log = setup_logger()

ROOT = Path(__file__).resolve().parent.parent
PRODUCTS_CONFIG = ROOT / "config" / "products.json"


def load_products() -> dict:
    with open(PRODUCTS_CONFIG, encoding="utf-8") as f:
        return json.load(f)


def list_fields(app_token: str, table_id: str, access_token: str) -> list[dict]:
    """列出表的所有字段"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    result = resp.json()
    if result.get("code") != 0:
        raise Exception(f"获取字段列表失败: {result}")
    return result["data"]["items"]


def fetch_all_records(app_token: str, table_id: str, access_token: str) -> list[dict]:
    """分页获取所有记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    all_records = []
    page_token = None

    while True:
        payload = {"page_size": 500}
        if page_token:
            payload["page_token"] = page_token

        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        result = resp.json()

        if result.get("code") == 429:
            log.warning("速率限制，等待 2 秒...")
            time.sleep(2)
            continue

        if result.get("code") != 0:
            raise Exception(f"查询记录失败: {result}")

        items = result.get("data", {}).get("items", [])
        all_records.extend(items)

        if not result.get("data", {}).get("has_more"):
            break
        page_token = result["data"]["page_token"]
        time.sleep(0.3)

    return all_records


def update_record(app_token: str, table_id: str, record_id: str, access_token: str, fields: dict):
    """更新单条记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    resp = requests.put(url, headers=headers, json={"fields": fields}, timeout=30)
    result = resp.json()

    if result.get("code") == 429:
        time.sleep(2)
        resp = requests.put(url, headers=headers, json={"fields": fields}, timeout=30)
        result = resp.json()

    if result.get("code") != 0:
        raise Exception(f"更新记录失败: {result}")

    return result


def migrate_group(group_name: str, group_config: dict, access_token: str, dry_run: bool = False):
    """迁移一个项目组的飞书表"""
    app_token = group_config["app_token"]
    table_id = group_config["table_id"]
    product_names = group_config["品名"]

    # 当前每个项目组只有一个品名，直接用
    if len(product_names) != 1:
        log.warning(f"[{group_name}] 有 {len(product_names)} 个品名，跳过自动迁移（需手动处理）")
        return
    product_name = product_names[0]

    print(f"\n{'='*60}")
    print(f"项目组: {group_name}")
    print(f"品名: {product_name}")
    print(f"app_token: {app_token}")
    print(f"table_id: {table_id}")
    print(f"{'='*60}")

    # Step 1: 检查是否已有"品名"字段
    fields = list_fields(app_token, table_id, access_token)
    field_names = [f["field_name"] for f in fields]

    if "品名" in field_names:
        print("  品名字段已存在，跳过创建")
    else:
        if dry_run:
            print("  [预览] 将添加「品名」文本字段")
        else:
            add_field(app_token, table_id, "品名", 1, access_token)
            print("  ✅ 已添加「品名」字段")

    # Step 2: 获取所有记录
    records = fetch_all_records(app_token, table_id, access_token)
    print(f"  共 {len(records)} 条记录")

    # Step 3: 遍历记录，迁移 unique_key 并填入品名
    updated = 0
    skipped = 0

    for record in records:
        record_id = record["record_id"]
        fields = record.get("fields", {})
        raw_key = fields.get("unique_key", "")

        # 飞书文本字段可能是 [{"text": "...", "type": "text"}] 或纯字符串
        if isinstance(raw_key, list) and raw_key:
            old_key = raw_key[0].get("text", "") if isinstance(raw_key[0], dict) else str(raw_key[0])
        elif isinstance(raw_key, str):
            old_key = raw_key
        else:
            skipped += 1
            continue

        # 判断是否已经迁移过（key 中已有 3 段）
        if old_key.count("|") >= 2:
            skipped += 1
            continue

        # 判断是否有 unique_key
        if not old_key or "|" not in old_key:
            skipped += 1
            continue

        # 构造新 unique_key: 日期|国家|品名
        new_key = f"{old_key}|{product_name}"

        if dry_run:
            print(f"  [预览] {old_key} → {new_key}")
            updated += 1
        else:
            update_record(app_token, table_id, record_id, access_token, {
                "unique_key": new_key,
                "品名": product_name,
            })
            updated += 1
            time.sleep(0.2)

    print(f"  {'预览' if dry_run else '迁移完成'}: 更新 {updated} 条，跳过 {skipped} 条")


def main():
    parser = argparse.ArgumentParser(description="迁移飞书表：添加品名字段 + 迁移 unique_key")
    parser.add_argument("--dry-run", action="store_true", help="预览模式")
    args = parser.parse_args()

    products = load_products()
    feishu_config = load_feishu_config()
    access_token = get_tenant_access_token(feishu_config["app_id"], feishu_config["app_secret"])

    print(f"🔄 飞书多维表格迁移 {'[预览模式]' if args.dry_run else '[执行模式]'}")
    print(f"   共 {len(products)} 个项目组")

    for group_name, group_config in products.items():
        try:
            migrate_group(group_name, group_config, access_token, dry_run=args.dry_run)
        except Exception as e:
            log.error(f"[{group_name}] 迁移失败: {e}")

    print(f"\n{'📋 预览完成' if args.dry_run else '✅ 全部迁移完成'}")


if __name__ == "__main__":
    main()
