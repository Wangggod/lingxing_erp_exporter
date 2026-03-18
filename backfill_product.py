"""批量回填产品数据：对已有的 raw 数据执行 process → preprocess → aggregate → upload"""

import argparse
import json
import sys
import pandas as pd
from pathlib import Path
from scripts.processor import process_date
from scripts.preprocessor import preprocess_product_data
from scripts.aggregator import aggregate_product_data
from scripts.fbm_rates import update_rates
from scripts.bitable_uploader import upload_summary_to_bitable
from scripts.logger import setup_logger

log = setup_logger()

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
PRODUCTS_CONFIG = ROOT / "config" / "products.json"


def load_products() -> dict:
    with open(PRODUCTS_CONFIG, encoding="utf-8") as f:
        return json.load(f)


def backfill(group_name: str, group_config: dict, upload: bool = True):
    product_names = group_config["品名"]
    bitable_config = {"app_token": group_config["app_token"], "table_id": group_config["table_id"]}

    # 获取所有可用日期并排序
    dates = sorted([d.name for d in RAW_DIR.iterdir() if d.is_dir()])
    log.info(f"回填项目组: {group_name}，品名: {product_names}，共 {len(dates)} 天 ({dates[0]} ~ {dates[-1]})")

    success = 0
    skipped = 0
    failed = 0

    for date_str in dates:
        log.info(f"\n{'='*60}")
        log.info(f"处理日期: {date_str} | 项目组: {group_name}")
        log.info(f"{'='*60}")

        try:
            # 1. 筛选
            process_date(date_str, group_name, product_names)

            # 2. 预处理
            product_dir = PROCESSED_DIR / date_str / group_name
            if not product_dir.exists():
                log.warning(f"[{date_str}] 筛选后无数据，跳过")
                skipped += 1
                continue
            preprocess_product_data(product_dir, date_str)

            # 3. FBM 费率更新
            fbm_path = RAW_DIR / date_str / "fbm_shipment.xlsx"
            if fbm_path.exists():
                update_rates(fbm_path)

            # 4. 聚合
            feishu_ready_dir = PROCESSED_DIR / date_str / "feishu-ready" / group_name
            aggregate_product_data(feishu_ready_dir, date_str)

            # 5. 上传
            if upload:
                csv_path = feishu_ready_dir / "daily_summary.csv"
                upload_summary_to_bitable(csv_path, bitable_config, force=True)

            # 6. 更新全产品合并 JSON
            update_daily_json(date_str, group_name, feishu_ready_dir / "daily_summary.csv")

            success += 1

        except Exception:
            log.exception(f"[{date_str}] 处理失败")
            failed += 1

    log.info(f"\n{'='*60}")
    log.info(f"回填完成: 成功 {success} / 跳过 {skipped} / 失败 {failed} / 共 {len(dates)}")
    log.info(f"{'='*60}")


def update_daily_json(date_str: str, group_name: str, csv_path: Path):
    """读取该项目组的聚合 CSV，合并写入当天的 daily_summary.json。"""
    json_path = PROCESSED_DIR / date_str / "daily_summary.json"

    # 读取已有 JSON（可能其他项目组已写入）
    if json_path.exists():
        with open(json_path, encoding="utf-8") as f:
            combined = json.load(f)
    else:
        combined = {"date": date_str, "groups": {}}

    # 兼容旧格式（"products" → "groups"）
    if "products" in combined and "groups" not in combined:
        combined["groups"] = combined.pop("products")

    # 添加/更新当前项目组
    df = pd.read_csv(csv_path)
    combined["groups"][group_name] = df.to_dict(orient="records")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    log.info(f"[{group_name}] JSON 已更新: {json_path}")


def main():
    parser = argparse.ArgumentParser(description="批量回填项目组数据")
    parser.add_argument("group_name", help="项目组名称")
    parser.add_argument("--no-upload", action="store_true", help="只处理不上传")
    args = parser.parse_args()

    products = load_products()
    if args.group_name not in products:
        log.error(f"项目组 '{args.group_name}' 未在 products.json 中注册")
        sys.exit(1)

    backfill(args.group_name, products[args.group_name], upload=not args.no_upload)


if __name__ == "__main__":
    main()
