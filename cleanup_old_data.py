"""数据清理脚本 - 删除过期的原始数据和处理后数据"""

import argparse
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from scripts.logger import setup_logger

log = setup_logger()


def cleanup_old_data(
    raw_retention_days: int = 30,
    processed_retention_days: int = 90,
    dry_run: bool = False
):
    """
    清理过期数据。

    Args:
        raw_retention_days: 原始数据保留天数
        processed_retention_days: 处理后数据保留天数
        dry_run: 是否为试运行（只显示将删除的内容，不实际删除）
    """
    log.info("=" * 60)
    log.info("开始数据清理")
    log.info("=" * 60)
    log.info(f"原始数据保留: {raw_retention_days} 天")
    log.info(f"处理后数据保留: {processed_retention_days} 天")
    if dry_run:
        log.info("⚠️  试运行模式（不会实际删除）")
    log.info("=" * 60)

    # 计算截止日期
    today = datetime.now()
    raw_cutoff = today - timedelta(days=raw_retention_days)
    processed_cutoff = today - timedelta(days=processed_retention_days)

    # 统计
    deleted_raw_count = 0
    deleted_processed_count = 0
    freed_space = 0

    # 清理原始数据
    raw_dir = Path("data/raw")
    if raw_dir.exists():
        log.info("\n清理原始数据...")
        for date_dir in sorted(raw_dir.iterdir()):
            if not date_dir.is_dir():
                continue

            try:
                date_str = date_dir.name
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")

                if date_obj < raw_cutoff:
                    # 计算大小
                    dir_size = sum(f.stat().st_size for f in date_dir.rglob('*') if f.is_file())
                    size_mb = dir_size / (1024 * 1024)

                    log.info(f"  删除: {date_str} ({size_mb:.2f} MB)")

                    if not dry_run:
                        shutil.rmtree(date_dir)

                    deleted_raw_count += 1
                    freed_space += dir_size

            except (ValueError, OSError) as e:
                log.warning(f"  跳过: {date_dir.name} ({e})")

    # 清理处理后数据
    processed_dir = Path("data/processed")
    if processed_dir.exists():
        log.info("\n清理处理后数据...")
        for date_dir in sorted(processed_dir.iterdir()):
            if not date_dir.is_dir():
                continue

            try:
                date_str = date_dir.name
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")

                if date_obj < processed_cutoff:
                    # 计算大小
                    dir_size = sum(f.stat().st_size for f in date_dir.rglob('*') if f.is_file())
                    size_mb = dir_size / (1024 * 1024)

                    log.info(f"  删除: {date_str} ({size_mb:.2f} MB)")

                    if not dry_run:
                        shutil.rmtree(date_dir)

                    deleted_processed_count += 1
                    freed_space += dir_size

            except (ValueError, OSError) as e:
                log.warning(f"  跳过: {date_dir.name} ({e})")

    # 总结
    freed_space_mb = freed_space / (1024 * 1024)
    freed_space_gb = freed_space_mb / 1024

    log.info("=" * 60)
    log.info("清理完成")
    log.info("=" * 60)
    log.info(f"删除原始数据目录: {deleted_raw_count}")
    log.info(f"删除处理后数据目录: {deleted_processed_count}")
    if freed_space_gb >= 1:
        log.info(f"释放空间: {freed_space_gb:.2f} GB")
    else:
        log.info(f"释放空间: {freed_space_mb:.2f} MB")
    log.info("=" * 60)

    if dry_run:
        log.info("⚠️  这是试运行，没有实际删除任何文件")
        log.info("   移除 --dry-run 参数以实际删除")


def main():
    parser = argparse.ArgumentParser(description="清理过期数据")
    parser.add_argument(
        "--raw-days",
        type=int,
        default=30,
        help="原始数据保留天数（默认: 30）"
    )
    parser.add_argument(
        "--processed-days",
        type=int,
        default=90,
        help="处理后数据保留天数（默认: 90）"
    )
    parser.add_argument(
        "--days",
        type=int,
        help="同时设置原始和处理后数据保留天数"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="试运行模式（不实际删除）"
    )

    args = parser.parse_args()

    # 如果指定了 --days，则同时设置两者
    raw_days = args.days if args.days is not None else args.raw_days
    processed_days = args.days if args.days is not None else args.processed_days

    try:
        cleanup_old_data(
            raw_retention_days=raw_days,
            processed_retention_days=processed_days,
            dry_run=args.dry_run
        )
    except Exception:
        log.exception("清理失败")
        raise


if __name__ == "__main__":
    main()
