"""上传每日汇总数据到飞书多维表格"""

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.bitable_uploader import upload_summary_to_bitable
from scripts.logger import setup_logger

log = setup_logger()

ROOT = Path(__file__).resolve().parent
PRODUCTS_CONFIG = ROOT / "config" / "products.json"


def get_target_date() -> str:
    """取美西时间当前日期的前一天，返回 YYYY-MM-DD。"""
    now_shanghai = datetime.now(ZoneInfo("Asia/Shanghai"))
    now_la = now_shanghai.astimezone(ZoneInfo("America/Los_Angeles"))
    return (now_la - timedelta(days=1)).strftime("%Y-%m-%d")


def load_products() -> dict:
    with open(PRODUCTS_CONFIG, encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="上传每日汇总数据到飞书多维表格")
    parser.add_argument(
        "--force",
        action="store_true",
        help="强制上传（忽略已上传标记）"
    )
    parser.add_argument(
        "--date",
        help="指定日期（YYYY-MM-DD），默认为昨天"
    )
    parser.add_argument(
        "--product",
        help="指定产品名（默认处理所有产品）"
    )
    args = parser.parse_args()

    products = load_products()
    DATE = args.date if args.date else get_target_date()

    # 支持只处理单个产品
    if args.product:
        if args.product not in products:
            log.error(f"产品 '{args.product}' 未在 products.json 中注册")
            return
        product_list = {args.product: products[args.product]}
    else:
        product_list = products

    for product_name, bitable_config in product_list.items():
        csv_path = Path(f"data/processed/{DATE}/feishu-ready/{product_name}/daily_summary.csv")

        if not csv_path.exists():
            log.warning(f"[{product_name}] 文件不存在: {csv_path}，跳过")
            continue

        log.info("=" * 60)
        log.info(f"开始上传数据到多维表格")
        log.info(f"日期: {DATE}")
        log.info(f"产品: {product_name}")
        log.info(f"文件: {csv_path}")
        if args.force:
            log.info("⚠️  强制上传模式")
        log.info("=" * 60)

        try:
            result = upload_summary_to_bitable(csv_path, bitable_config, force=args.force)

            print(f"\n[{product_name}] " + "=" * 50)
            if result.get("skipped"):
                print("⏭️  跳过上传（已上传过）")
                print(f"原因: {result.get('reason')}")
                print("使用 --force 参数强制重新上传")
            else:
                print("🎉 上传完成！")
                print(f"总记录数: {result['total']}")
                print(f"创建: {result.get('created', 0)}")
                print(f"更新: {result.get('updated', 0)}")
                print(f"失败: {result['failed']}")
            print("=" * 60)

        except Exception as e:
            log.error(f"[{product_name}] 上传失败: {e}")
            raise


if __name__ == "__main__":
    main()
