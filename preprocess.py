"""数据预处理入口：为上传飞书准备数据"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.preprocessor import preprocess_product_data
from scripts.logger import setup_logger

log = setup_logger()

ROOT = Path(__file__).resolve().parent
PRODUCTS_CONFIG = ROOT / "config" / "products.json"
PROCESSED_DIR = ROOT / "data" / "processed"


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

    for group_name in products:
        log.info(f"开始预处理数据 - 日期: {target_date}, 项目组: {group_name}")

        product_dir = PROCESSED_DIR / target_date / group_name

        if not product_dir.exists():
            log.warning(f"[{group_name}] 项目组数据目录不存在: {product_dir}，跳过")
            continue

        try:
            saved_files = preprocess_product_data(product_dir, target_date)

            log.info(f"[{group_name}] 预处理完成！保存的文件:")
            for file_type, path in saved_files.items():
                log.info(f"  [{file_type}] {path}")

        except Exception:
            log.exception(f"[{group_name}] 数据预处理失败")
            raise


if __name__ == "__main__":
    main()
