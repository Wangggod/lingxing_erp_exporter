#!/usr/bin/env python3
"""
同步 ERP 订单数据到 SellerGhost

读取 lingxing_erp_exporter 处理好的 CSV，按产品配置调用 SellerGhost API 批量导入订单。
服务端 upsert 保证幂等，重复同步不会出错。

用法:
    python sync_to_sellerghost.py                     # 同步当天
    python sync_to_sellerghost.py --date 2026-03-17   # 指定日期
    python sync_to_sellerghost.py --days 7            # 回填7天
    python sync_to_sellerghost.py --dry-run           # 预览不执行
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# 项目根目录
PROJECT_DIR = Path(__file__).parent
CONFIG_PATH = PROJECT_DIR / "config" / "sellerghost.json"
DATA_DIR = PROJECT_DIR / "data" / "processed"

# 每批最多提交的订单数（API 限制 5000）
BATCH_SIZE = 4000


def load_config():
    """加载 SellerGhost 配置"""
    if not CONFIG_PATH.exists():
        print(f"❌ 配置文件不存在: {CONFIG_PATH}")
        print(f"   请复制 config/sellerghost.example.json 并填入实际值")
        sys.exit(1)

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    # 检查是否有未填写的 TODO
    for product_name, product_config in config.get("products", {}).items():
        if "TODO" in product_config.get("admin_key", ""):
            print(f"⚠️  产品 [{product_name}] 的 admin_key 未配置，跳过")

    return config


def read_orders_from_csv(csv_path, product_id):
    """从 CSV 读取订单，映射为 SellerGhost API 格式"""
    orders = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_number = row.get("防重复编号", "").strip()
            if not order_number:
                continue

            order = {
                "orderNumber": order_number,
                "productId": product_id,
                "platform": "amazon",
            }

            # 可选字段
            buyer_name = row.get("买家姓名", "").strip()
            if buyer_name:
                order["buyerName"] = buyer_name

            asin = row.get("ASIN", "").strip()
            if asin:
                order["asin"] = asin

            orders.append(order)

    return orders


def sync_orders(api_base, admin_key, orders, dry_run=False):
    """批量同步订单到 SellerGhost"""
    url = f"{api_base}/api/admin/orders/batch"
    headers = {
        "Content-Type": "application/json",
        "X-Admin-Key": admin_key,
    }

    total = len(orders)
    synced = 0

    for i in range(0, total, BATCH_SIZE):
        batch = orders[i : i + BATCH_SIZE]

        if dry_run:
            print(f"    [预览] 批次 {i // BATCH_SIZE + 1}: {len(batch)} 条订单")
            synced += len(batch)
            continue

        try:
            resp = requests.post(url, json={"orders": batch}, headers=headers, timeout=30)
            resp.raise_for_status()
            result = resp.json()
            count = result.get("data", {}).get("count", 0)
            synced += count
            print(f"    ✅ 批次 {i // BATCH_SIZE + 1}: 同步 {count} 条")
        except requests.exceptions.RequestException as e:
            print(f"    ❌ 批次 {i // BATCH_SIZE + 1} 失败: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"       响应: {e.response.text[:200]}")

    return synced


def sync_date(config, date_str, dry_run=False):
    """同步指定日期的所有产品订单"""
    api_base = config["api_base"]
    products = config["products"]
    date_dir = DATA_DIR / date_str / "feishu-ready"

    if not date_dir.exists():
        print(f"  ⚠️  数据目录不存在: {date_dir}")
        return 0

    total_synced = 0

    for group_name, group_config in products.items():
        admin_key = group_config.get("admin_key", "")
        product_id = group_config.get("product_id", "")

        if "TODO" in admin_key or "TODO" in product_id:
            print(f"  ⏭️  [{group_name}] 未配置，跳过")
            continue

        csv_path = date_dir / group_name / "order_list_ready.csv"
        if not csv_path.exists():
            print(f"  ⏭️  [{group_name}] CSV 不存在: {csv_path.name}")
            continue

        orders = read_orders_from_csv(csv_path, product_id)
        if not orders:
            print(f"  ⏭️  [{group_name}] 无有效订单")
            continue

        print(f"  📦 [{group_name}] 共 {len(orders)} 条订单")
        synced = sync_orders(api_base, admin_key, orders, dry_run=dry_run)
        total_synced += synced

    return total_synced


def main():
    parser = argparse.ArgumentParser(description="同步 ERP 订单到 SellerGhost")
    parser.add_argument("--date", help="指定日期 (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="回填最近N天")
    parser.add_argument("--dry-run", action="store_true", help="预览模式，不实际提交")
    args = parser.parse_args()

    config = load_config()

    # 确定要同步的日期列表
    if args.days:
        now_la = datetime.now(ZoneInfo("Asia/Shanghai")).astimezone(ZoneInfo("America/Los_Angeles"))
        base = now_la - timedelta(days=1)
        dates = [
            (base - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(args.days)
        ]
    elif args.date:
        dates = [args.date]
    else:
        # 与 ETL 保持一致：美西时间前一天
        now_la = datetime.now(ZoneInfo("Asia/Shanghai")).astimezone(ZoneInfo("America/Los_Angeles"))
        dates = [(now_la - timedelta(days=1)).strftime("%Y-%m-%d")]

    mode = "预览模式" if args.dry_run else "同步模式"
    print(f"\n🔄 SellerGhost 订单同步 [{mode}]")
    print(f"   API: {config['api_base']}")
    print(f"   日期: {', '.join(dates)}\n")

    grand_total = 0
    for date_str in dates:
        print(f"📅 {date_str}")
        synced = sync_date(config, date_str, dry_run=args.dry_run)
        grand_total += synced
        print()

    print(f"{'📋 预览' if args.dry_run else '✅ 完成'}: 共 {grand_total} 条订单")


if __name__ == "__main__":
    main()
