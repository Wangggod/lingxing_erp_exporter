"""创建飞书多维表格用于存储每日汇总数据

用法:
    python tools/create_bitable.py 保险箱 --品名 艾洛克保险箱TX9S,艾洛克保险箱WX001,种子链接保险箱
    python tools/create_bitable.py 猫砂盆 --品名 半开猫砂盆,猫砂盆-中山1号
"""

import argparse
import json
import sys
from pathlib import Path

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.bitable_helper import create_summary_bitable
from scripts.logger import setup_logger

log = setup_logger()

ROOT = Path(__file__).resolve().parent.parent
PRODUCTS_CONFIG = ROOT / "config" / "products.json"


def load_products() -> dict:
    if PRODUCTS_CONFIG.exists():
        with open(PRODUCTS_CONFIG, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_products(products: dict):
    with open(PRODUCTS_CONFIG, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
        f.write("\n")


def main():
    parser = argparse.ArgumentParser(description="创建飞书多维表格并注册到 products.json")
    parser.add_argument("group_name", help="项目组名称（如 保险箱、猫砂盆）")
    parser.add_argument("--品名", required=True, help="品名列表（逗号分隔，与领星报表品名一致）")
    args = parser.parse_args()

    group_name = args.group_name
    product_names = [n.strip() for n in args.品名.split(",")]
    products = load_products()

    if group_name in products:
        print(f"⚠️  项目组 '{group_name}' 已在 products.json 中注册:")
        print(f"   app_token: {products[group_name]['app_token']}")
        print(f"   table_id: {products[group_name]['table_id']}")
        print(f"   品名: {products[group_name]['品名']}")
        print("如需重新创建，请先从 products.json 中删除该项目组")
        return

    log.info("=" * 60)
    log.info(f"开始创建多维表格: {group_name}-每日汇总")
    log.info(f"包含品名: {product_names}")
    log.info("=" * 60)

    try:
        result = create_summary_bitable(group_name)

        # 注册到 products.json
        products[group_name] = {
            "app_token": result["app_token"],
            "table_id": result["table_id"],
            "品名": product_names
        }
        save_products(products)

        print("\n" + "=" * 60)
        print("🎉 创建成功！")
        print("=" * 60)
        print(f"项目组名称: {group_name}")
        print(f"包含品名: {product_names}")
        print(f"app_token: {result['app_token']}")
        print(f"table_id: {result['table_id']}")
        print(f"\n访问链接: {result['url']}")
        print(f"\n✅ 已注册到 {PRODUCTS_CONFIG}")
        print("=" * 60)

    except Exception as e:
        log.error(f"创建失败: {e}")
        raise


if __name__ == "__main__":
    main()
