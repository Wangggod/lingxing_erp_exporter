"""回填2月份所有缺失的原始数据，然后走完整 ETL 流程。

用法：
    ./venv/bin/python3 backfill_february.py
    ./venv/bin/python3 backfill_february.py --skip-download   # 跳过下载，只处理+聚合+上传
    ./venv/bin/python3 backfill_february.py --skip-upload     # 跳过上传飞书
"""

import argparse
from pathlib import Path

from playwright.sync_api import sync_playwright

from scripts.logger import setup_logger
from scripts.exporter import (
    load_config, _create_context, _ensure_logged_in, _navigate_to_app,
    export_order_profit, export_order_list, export_fbm_shipment,
    download_report, SessionExpiredError, STATE_FILE,
)
from scripts.processor import process_date
from scripts.preprocessor import preprocess_product_data
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

# 2月全部日期
ALL_FEB_DATES = [f"2026-02-{d:02d}" for d in range(1, 29)]
# 3月已有日期
ALL_MAR_DATES = [f"2026-03-{d:02d}" for d in range(1, 7)]
# 全部日期
ALL_DATES = ALL_FEB_DATES + ALL_MAR_DATES

# 报表类型 → (导出函数, 文件名)
REPORT_HANDLERS = {
    "order_profit": export_order_profit,
    "order_list": export_order_list,
    "fbm_shipment": export_fbm_shipment,
}


def scan_missing(dates: list[str]) -> dict[str, list[str]]:
    """扫描每个日期缺少哪些报表文件，返回 {date: [missing_types]}。"""
    missing = {}
    for date_str in dates:
        date_dir = RAW_DIR / date_str
        needed = []
        for rtype in REPORT_HANDLERS:
            if not (date_dir / f"{rtype}.xlsx").exists():
                needed.append(rtype)
        if needed:
            missing[date_str] = needed
    return missing


def download_missing(missing: dict[str, list[str]]) -> None:
    """批量下载所有缺失的报表。"""
    cfg = load_config()
    reports_by_type = {r["type"]: r for r in cfg["reports"]}

    # 统计总任务数
    total = sum(len(types) for types in missing.values())
    log.info(f"共需下载 {total} 个文件，涉及 {len(missing)} 天")

    done = 0

    for attempt in range(2):
        with sync_playwright() as pw:
            ctx = _create_context(pw, cfg)
            page = ctx.new_page()
            try:
                _ensure_logged_in(page, ctx, cfg)
                _navigate_to_app(page, cfg)

                for date_str, types in sorted(missing.items()):
                    for rtype in types:
                        dest = RAW_DIR / date_str / f"{rtype}.xlsx"
                        if dest.exists():
                            done += 1
                            continue

                        report = reports_by_type.get(rtype)
                        if not report:
                            log.error(f"config.json 中未找到报表类型 {rtype}，跳过")
                            done += 1
                            continue

                        handler = REPORT_HANDLERS[rtype]
                        done += 1
                        log.info(f"[{done}/{total}] {date_str} / {rtype} 下载中...")

                        try:
                            report_id = handler(page, report, date_str)
                            download_report(page, cfg, report_id, report["name"], rtype, date_str)
                        except Exception as e:
                            log.error(f"[{date_str}/{rtype}] 下载失败: {e}")
                            raise

                break  # 全部成功

            except SessionExpiredError:
                if attempt == 0:
                    log.warning("会话失效，清除登录状态后重试...")
                    if STATE_FILE.exists():
                        STATE_FILE.unlink()
                    # 重置 done 计数，重新开始（已下载的会被跳过）
                    done = 0
                    continue
                raise
            finally:
                ctx.close()


def process_all(dates: list[str], skip_upload: bool = False) -> None:
    """对所有日期执行 筛选 → 预处理 → 更新费率表 → 聚合 → 上传。"""
    for date_str in sorted(dates):
        raw_dir = RAW_DIR / date_str
        if not raw_dir.exists():
            log.warning(f"[{date_str}] 原始数据目录不存在，跳过")
            continue

        log.info(f"{'='*60}")
        log.info(f"处理 {date_str}")
        log.info(f"{'='*60}")

        try:
            # 1. 筛选
            process_date(date_str, PRODUCT_NAME)

            # 2. 预处理
            product_dir = PROCESSED_DIR / date_str / PRODUCT_NAME
            if product_dir.exists():
                preprocess_product_data(product_dir, date_str)

            # 3. 更新 FBM 费率表
            fbm_path = raw_dir / "fbm_shipment.xlsx"
            if fbm_path.exists():
                update_rates(fbm_path)

            # 4. 聚合
            feishu_dir = PROCESSED_DIR / date_str / "feishu-ready" / PRODUCT_NAME
            if feishu_dir.exists():
                output_path = aggregate_product_data(feishu_dir, date_str)

                # 5. 上传
                if not skip_upload:
                    success_file = output_path.parent / "daily_summary.success"
                    if success_file.exists():
                        success_file.unlink()
                    result = upload_summary_to_bitable(output_path, BITABLE_CONFIG, force=True)
                    c, u, f = result.get("created", 0), result.get("updated", 0), result.get("failed", 0)
                    log.info(f"[{date_str}] 上传: 创建={c}, 更新={u}, 失败={f}")

        except Exception as e:
            log.error(f"[{date_str}] 处理失败: {e}")
            # 继续处理下一天
            continue


def main():
    parser = argparse.ArgumentParser(description="回填2月份所有缺失数据")
    parser.add_argument("--skip-download", action="store_true", help="跳过下载")
    parser.add_argument("--skip-upload", action="store_true", help="跳过上传飞书")
    args = parser.parse_args()

    # 扫描缺失
    missing = scan_missing(ALL_DATES)
    if missing:
        total_files = sum(len(t) for t in missing.values())
        log.info(f"缺失数据: {len(missing)} 天，共 {total_files} 个文件")
        for d, types in sorted(missing.items()):
            log.info(f"  {d}: {', '.join(types)}")
    else:
        log.info("数据已完整，无需下载")

    # 下载
    if not args.skip_download and missing:
        log.info("=" * 60)
        log.info("第一步：批量下载缺失报表")
        log.info("=" * 60)
        download_missing(missing)

    # 处理（所有日期都重新走一遍，确保一致）
    log.info("=" * 60)
    log.info("第二步：处理 + 聚合 + 上传")
    log.info("=" * 60)
    process_all(ALL_DATES, skip_upload=args.skip_upload)

    log.info("=" * 60)
    log.info("数据回填全部完成！")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
