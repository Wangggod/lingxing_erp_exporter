"""批量处理历史数据：下载、处理、上传"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from scripts.exporter import run_for_date
from scripts.processor import process_date
from scripts.preprocessor import preprocess_product_data
from scripts.aggregator import aggregate_product_data
from scripts.bitable_uploader import upload_summary_to_bitable
from scripts.logger import setup_logger

log = setup_logger()


def process_single_date(date_str: str, product_name: str, bitable_config: dict):
    """处理单个日期的完整流程"""
    log.info("=" * 60)
    log.info(f"开始处理日期: {date_str}")
    log.info("=" * 60)

    try:
        # 1. 下载（如果还没下载）
        raw_dir = Path("data/raw") / date_str
        if not raw_dir.exists() or not (raw_dir / "order_profit.xlsx").exists():
            log.info(f"[{date_str}] 开始下载数据...")
            run_for_date(date_str)
        else:
            log.info(f"[{date_str}] 原始数据已存在，跳过下载")

        # 2. 筛选
        log.info(f"[{date_str}] 筛选产品数据...")
        process_date(date_str, product_name)

        # 3. 预处理
        log.info(f"[{date_str}] 预处理数据...")
        product_dir = Path("data/processed") / date_str / product_name
        preprocess_product_data(product_dir, date_str)

        # 4. 聚合
        log.info(f"[{date_str}] 聚合数据...")
        feishu_ready_dir = Path("data/processed") / date_str / "feishu-ready" / product_name
        aggregate_product_data(feishu_ready_dir, date_str)

        # 5. 上传
        log.info(f"[{date_str}] 上传到多维表格...")
        csv_path = feishu_ready_dir / "daily_summary.csv"
        upload_summary_to_bitable(csv_path, bitable_config)

        log.info(f"✅ [{date_str}] 处理完成")
        return True

    except Exception as e:
        log.error(f"❌ [{date_str}] 处理失败: {e}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="批量处理历史数据")
    parser.add_argument("--start", required=True, help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--product", default="半开猫砂盆", help="产品名称")

    args = parser.parse_args()

    # 多维表格配置
    bitable_config = {
        "app_token": "MsYxbyF7yak7TGsZwrgc3SWunSb",
        "table_id": "tbl94y30jp2DTTHu"
    }

    # 解析日期范围
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    # 生成日期列表
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    log.info("=" * 60)
    log.info(f"批量处理计划")
    log.info("=" * 60)
    log.info(f"日期范围: {args.start} ~ {args.end}")
    log.info(f"产品: {args.product}")
    log.info(f"总天数: {len(dates)}")
    log.info("=" * 60)

    # 处理每一天
    success_count = 0
    failed_dates = []

    for date_str in dates:
        success = process_single_date(date_str, args.product, bitable_config)
        if success:
            success_count += 1
        else:
            failed_dates.append(date_str)

    # 总结
    log.info("=" * 60)
    log.info("批量处理完成")
    log.info("=" * 60)
    log.info(f"成功: {success_count}/{len(dates)}")
    if failed_dates:
        log.info(f"失败的日期: {', '.join(failed_dates)}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
