"""批量下载指定日期范围的原始报表数据，复用同一个浏览器会话。"""

import argparse
import time
from datetime import datetime, timedelta
from scripts.exporter import (
    load_config, _create_context, _ensure_logged_in,
    _navigate_to_app, _run_exports, STATE_FILE, SessionExpiredError,
)
from scripts.logger import setup_logger
from playwright.sync_api import sync_playwright

log = setup_logger()


def date_range(start: str, end: str):
    """生成 [start, end] 闭区间的日期字符串列表。"""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    dates = []
    while s <= e:
        dates.append(s.strftime("%Y-%m-%d"))
        s += timedelta(days=1)
    return dates


def batch_download(start_date: str, end_date: str):
    dates = date_range(start_date, end_date)
    log.info(f"批量下载: {start_date} ~ {end_date}，共 {len(dates)} 天")

    cfg = load_config()
    success = 0
    failed = 0
    failed_dates = []

    with sync_playwright() as pw:
        ctx = _create_context(pw, cfg)
        page = ctx.new_page()

        try:
            _ensure_logged_in(page, ctx, cfg)
            _navigate_to_app(page, cfg)

            for i, target_date in enumerate(dates):
                log.info(f"\n[{i+1}/{len(dates)}] 下载日期: {target_date}")
                try:
                    _run_exports(page, cfg, target_date)
                    success += 1
                    # 每天之间间隔几秒，避免限频
                    if i < len(dates) - 1:
                        time.sleep(5)
                except SessionExpiredError:
                    log.warning("会话失效，重新登录...")
                    if STATE_FILE.exists():
                        STATE_FILE.unlink()
                    ctx.close()
                    ctx = _create_context(pw, cfg)
                    page = ctx.new_page()
                    _ensure_logged_in(page, ctx, cfg)
                    _navigate_to_app(page, cfg)
                    # 重试当天
                    try:
                        _run_exports(page, cfg, target_date)
                        success += 1
                    except Exception:
                        log.exception(f"[{target_date}] 重新登录后仍失败")
                        failed += 1
                        failed_dates.append(target_date)
                except Exception:
                    log.exception(f"[{target_date}] 下载失败")
                    failed += 1
                    failed_dates.append(target_date)
                    # 短暂等待后继续下一天
                    time.sleep(10)
        finally:
            ctx.close()

    log.info(f"\n{'='*60}")
    log.info(f"批量下载完成: 成功 {success} / 失败 {failed} / 共 {len(dates)}")
    if failed_dates:
        log.info(f"失败日期: {', '.join(failed_dates)}")
    log.info(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="批量下载原始报表")
    parser.add_argument("--start", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")
    args = parser.parse_args()
    batch_download(args.start, args.end)


if __name__ == "__main__":
    main()
