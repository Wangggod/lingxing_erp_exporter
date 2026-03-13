"""数据聚合入口：生成每日汇总数据"""

import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.aggregator import aggregate_product_data
from scripts.fbm_rates import update_rates
from scripts.logger import setup_logger

log = setup_logger()

ROOT = Path(__file__).resolve().parent
PRODUCTS_CONFIG = ROOT / "config" / "products.json"
PROCESSED_DIR = ROOT / "data" / "processed"
RAW_DIR = ROOT / "data" / "raw"


def get_target_date() -> str:
    """取美西时间当前日期的前一天，返回 YYYY-MM-DD。"""
    now_shanghai = datetime.now(ZoneInfo("Asia/Shanghai"))
    now_la = now_shanghai.astimezone(ZoneInfo("America/Los_Angeles"))
    return (now_la - timedelta(days=1)).strftime("%Y-%m-%d")


def load_products() -> dict:
    with open(PRODUCTS_CONFIG, encoding="utf-8") as f:
        return json.load(f)


def main():
    products = load_products()
    target_date = get_target_date()

    # FBM 运费费率表更新只需一次（与产品无关）
    fbm_shipment_path = RAW_DIR / target_date / "fbm_shipment.xlsx"
    if fbm_shipment_path.exists():
        log.info("更新 FBM 运费费率表...")
        update_rates(fbm_shipment_path)
    else:
        log.info(f"无自发货订单文件 ({fbm_shipment_path})，跳过费率更新")

    aggregated_products = {}

    for product_name in products:
        log.info(f"开始聚合数据 - 日期: {target_date}, 产品: {product_name}")

        feishu_ready_dir = PROCESSED_DIR / target_date / "feishu-ready" / product_name

        if not feishu_ready_dir.exists():
            log.warning(f"[{product_name}] 数据目录不存在: {feishu_ready_dir}，跳过")
            continue

        try:
            output_path = aggregate_product_data(feishu_ready_dir, target_date)
            log.info(f"[{product_name}] 聚合完成！文件: {output_path}")

            # 读取聚合结果用于合并 JSON
            df = pd.read_csv(output_path)
            aggregated_products[product_name] = df.to_dict(orient="records")

        except Exception:
            log.exception(f"[{product_name}] 数据聚合失败")
            raise

    # 生成全产品合并 JSON
    if aggregated_products:
        combined = {"date": target_date, "products": aggregated_products}
        json_path = PROCESSED_DIR / target_date / "daily_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=2)
        log.info(f"全产品 JSON 已生成: {json_path}")


if __name__ == "__main__":
    main()
