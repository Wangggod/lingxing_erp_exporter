"""数据预处理入口：为上传飞书准备数据"""

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.preprocessor import preprocess_product_data
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
    ROOT = Path(__file__).resolve().parent
    PROCESSED_DIR = ROOT / "data" / "processed"

    # 获取目标日期
    target_date = get_target_date()

    log.info(f"开始预处理数据 - 日期: {target_date}, 产品: {PRODUCT_NAME}")

    # 产品数据目录
    product_dir = PROCESSED_DIR / target_date / PRODUCT_NAME

    if not product_dir.exists():
        log.error(f"产品数据目录不存在: {product_dir}")
        log.info("请先运行 process.py 生成筛选后的数据")
        return

    try:
        saved_files = preprocess_product_data(product_dir, target_date)

        log.info("预处理完成！保存的文件:")
        for file_type, path in saved_files.items():
            log.info(f"  [{file_type}] {path}")

    except Exception:
        log.exception("数据预处理失败")
        raise


if __name__ == "__main__":
    main()
