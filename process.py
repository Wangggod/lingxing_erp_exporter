"""数据处理入口：筛选特定产品的数据"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from scripts.processor import process_date
from scripts.logger import setup_logger

log = setup_logger()


def get_target_date() -> str:
    """取美西时间当前日期的前一天，返回 YYYY-MM-DD。"""
    now_shanghai = datetime.now(ZoneInfo("Asia/Shanghai"))
    now_la = now_shanghai.astimezone(ZoneInfo("America/Los_Angeles"))
    return (now_la - timedelta(days=1)).strftime("%Y-%m-%d")


def main():
    # 配置
    PRODUCT_NAME = "半开猫砂盆"

    # 获取目标日期（与下载逻辑保持一致）
    target_date = get_target_date()

    log.info(f"开始处理数据 - 日期: {target_date}, 产品: {PRODUCT_NAME}")

    try:
        saved_files = process_date(target_date, PRODUCT_NAME)

        log.info("处理完成！保存的文件:")
        for file_type, path in saved_files.items():
            log.info(f"  [{file_type}] {path}")

    except Exception:
        log.exception("数据处理失败")
        raise


if __name__ == "__main__":
    main()
