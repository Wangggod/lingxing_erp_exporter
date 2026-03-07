"""回填最近 N 天的自发货订单数据，更新费率表并重新聚合上传。

用法：
    ./venv/bin/python3 backfill_fbm.py          # 默认最近 5 天
    ./venv/bin/python3 backfill_fbm.py --days 10 # 最近 10 天
    ./venv/bin/python3 backfill_fbm.py --skip-download  # 跳过下载，只重新聚合
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.logger import setup_logger
from scripts.fbm_rates import update_rates
from scripts.aggregator import aggregate_product_data
from scripts.bitable_uploader import upload_summary_to_bitable

log = setup_logger()

ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
PRODUCT_NAME = "半开猫砂盆"
BITABLE_CONFIG = {
    "app_token": "MsYxbyF7yak7TGsZwrgc3SWunSb",
    "table_id": "tbl94y30jp2DTTHu",
}


def get_dates(days: int) -> list[str]:
    """获取最近 N 天的日期列表（美西时间）。"""
    now_la = datetime.now(ZoneInfo("Asia/Shanghai")).astimezone(ZoneInfo("America/Los_Angeles"))
    yesterday = now_la - timedelta(days=1)
    return [(yesterday - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)][::-1]


def download_fbm_shipments(dates: list[str]) -> None:
    """批量下载自发货订单报表。"""
    from playwright.sync_api import sync_playwright
    from scripts.exporter import (
        load_config, _create_context, _ensure_logged_in,
        _navigate_to_app, export_fbm_shipment, download_report,
        SessionExpiredError, STATE_FILE,
    )

    cfg = load_config()

    # 找到 fbm_shipment 报表配置
    fbm_report = None
    for r in cfg["reports"]:
        if r["type"] == "fbm_shipment":
            fbm_report = r
            break
    if not fbm_report:
        raise RuntimeError("config.json 中未找到 fbm_shipment 报表配置")

    for attempt in range(2):
        with sync_playwright() as pw:
            ctx = _create_context(pw, cfg)
            page = ctx.new_page()
            try:
                _ensure_logged_in(page, ctx, cfg)
                _navigate_to_app(page, cfg)

                for date_str in dates:
                    dest = RAW_DIR / date_str / "fbm_shipment.xlsx"
                    if dest.exists():
                        log.info(f"[{date_str}] fbm_shipment.xlsx 已存在，跳过")
                        continue

                    log.info(f"[{date_str}] 开始下载自发货订单...")
                    try:
                        report_id = export_fbm_shipment(page, fbm_report, date_str)
                        download_report(page, cfg, report_id, fbm_report["name"], "fbm_shipment", date_str)
                        log.info(f"[{date_str}] 下载完成")
                    except Exception as e:
                        log.error(f"[{date_str}] 下载失败: {e}")
                        raise

                break  # 全部成功

            except SessionExpiredError:
                if attempt == 0:
                    log.warning("会话失效，清除登录状态后重试...")
                    if STATE_FILE.exists():
                        STATE_FILE.unlink()
                    continue
                raise
            finally:
                ctx.close()


def update_rates_for_dates(dates: list[str]) -> None:
    """用各天的 fbm_shipment.xlsx 更新费率表。"""
    for date_str in dates:
        path = RAW_DIR / date_str / "fbm_shipment.xlsx"
        if path.exists():
            log.info(f"[{date_str}] 更新费率表...")
            added = update_rates(path)
            log.info(f"[{date_str}] 新增 {added} 条费率记录")
        else:
            log.info(f"[{date_str}] 无 fbm_shipment.xlsx，跳过")


def reaggregate_and_upload(dates: list[str]) -> None:
    """重新聚合并上传各天的数据。"""
    for date_str in dates:
        feishu_dir = PROCESSED_DIR / date_str / "feishu-ready" / PRODUCT_NAME
        if not feishu_dir.exists():
            log.warning(f"[{date_str}] 无预处理数据，跳过")
            continue

        log.info(f"[{date_str}] 重新聚合...")
        try:
            output_path = aggregate_product_data(feishu_dir, date_str)

            # 删除旧的 .success 标记以便重新上传
            success_file = output_path.parent / "daily_summary.success"
            if success_file.exists():
                success_file.unlink()

            log.info(f"[{date_str}] 上传到飞书多维表格...")
            result = upload_summary_to_bitable(output_path, BITABLE_CONFIG, force=True)
            created = result.get("created", 0)
            updated = result.get("updated", 0)
            failed = result.get("failed", 0)
            log.info(f"[{date_str}] 上传完成: 创建={created}, 更新={updated}, 失败={failed}")

        except Exception as e:
            log.error(f"[{date_str}] 聚合/上传失败: {e}")


def main():
    parser = argparse.ArgumentParser(description="回填 FBM 自发货订单数据")
    parser.add_argument("--days", type=int, default=5, help="回填天数（默认 5）")
    parser.add_argument("--skip-download", action="store_true", help="跳过下载，只更新费率表+重新聚合")
    parser.add_argument("--skip-upload", action="store_true", help="跳过上传飞书")
    args = parser.parse_args()

    dates = get_dates(args.days)
    log.info(f"回填日期范围: {dates[0]} ~ {dates[-1]}（共 {len(dates)} 天）")

    # 第一步：下载
    if not args.skip_download:
        log.info("=" * 60)
        log.info("第一步：批量下载自发货订单")
        log.info("=" * 60)
        download_fbm_shipments(dates)

    # 第二步：更新费率表
    log.info("=" * 60)
    log.info("第二步：更新 FBM 运费费率表")
    log.info("=" * 60)
    update_rates_for_dates(dates)

    # 第三步：重新聚合并上传
    if not args.skip_upload:
        log.info("=" * 60)
        log.info("第三步：重新聚合并上传")
        log.info("=" * 60)
        reaggregate_and_upload(dates)
    else:
        log.info("跳过上传（--skip-upload）")

    log.info("=" * 60)
    log.info("全部完成！")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
