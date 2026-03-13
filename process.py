"""数据处理入口：筛选特定产品的数据"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.processor import process_date
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
    products = load_products()
    target_date = get_target_date()

    for product_name in products:
        log.info(f"开始处理数据 - 日期: {target_date}, 产品: {product_name}")

        try:
            saved_files = process_date(target_date, product_name)

            log.info(f"[{product_name}] 处理完成！保存的文件:")
            for file_type, path in saved_files.items():
                log.info(f"  [{file_type}] {path}")

        except Exception:
            log.exception(f"[{product_name}] 数据处理失败")
            raise


if __name__ == "__main__":
    main()
