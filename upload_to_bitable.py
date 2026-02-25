"""上传每日汇总数据到飞书多维表格"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.bitable_uploader import upload_summary_to_bitable
from scripts.logger import setup_logger

log = setup_logger()


def get_target_date() -> str:
    """取美西时间当前日期的前一天，返回 YYYY-MM-DD。"""
    now_shanghai = datetime.now(ZoneInfo("Asia/Shanghai"))
    now_la = now_shanghai.astimezone(ZoneInfo("America/Los_Angeles"))
    return (now_la - timedelta(days=1)).strftime("%Y-%m-%d")


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
    args = parser.parse_args()

    # 配置
    PRODUCT_NAME = "半开猫砂盆"

    # 获取目标日期
    DATE = args.date if args.date else get_target_date()

    # 多维表格配置
    BITABLE_CONFIG = {
        "app_token": "MsYxbyF7yak7TGsZwrgc3SWunSb",
        "table_id": "tbl94y30jp2DTTHu"
    }

    # 输入文件
    csv_path = Path(f"data/processed/{DATE}/feishu-ready/{PRODUCT_NAME}/daily_summary.csv")

    if not csv_path.exists():
        log.error(f"文件不存在: {csv_path}")
        return

    log.info("=" * 60)
    log.info(f"开始上传数据到多维表格")
    log.info(f"日期: {DATE}")
    log.info(f"产品: {PRODUCT_NAME}")
    log.info(f"文件: {csv_path}")
    if args.force:
        log.info("⚠️  强制上传模式")
    log.info("=" * 60)

    try:
        result = upload_summary_to_bitable(csv_path, BITABLE_CONFIG, force=args.force)

        print("\n" + "=" * 60)
        if result.get("skipped"):
            print("⏭️  跳过上传（已上传过）")
            print("=" * 60)
            print(f"原因: {result.get('reason')}")
            print("使用 --force 参数强制重新上传")
        else:
            print("🎉 上传完成！")
            print("=" * 60)
            print(f"总记录数: {result['total']}")
            print(f"创建: {result.get('created', 0)}")
            print(f"更新: {result.get('updated', 0)}")
            print(f"失败: {result['failed']}")
        print("=" * 60)

    except Exception as e:
        log.error(f"上传失败: {e}")
        raise


if __name__ == "__main__":
    main()
