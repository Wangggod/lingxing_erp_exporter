"""上传数据到飞书入口"""

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.uploader import upload_to_feishu
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

    log.info(f"开始上传数据到飞书 - 日期: {target_date}, 产品: {PRODUCT_NAME}")

    # 预处理后的数据目录
    feishu_ready_dir = PROCESSED_DIR / target_date / "feishu-ready" / PRODUCT_NAME

    if not feishu_ready_dir.exists():
        log.error(f"数据目录不存在: {feishu_ready_dir}")
        log.info("请先运行 preprocess.py 生成预处理后的数据")
        return

    try:
        # 上传订单列表
        list_csv = feishu_ready_dir / "order_list_ready.csv"
        if list_csv.exists():
            log.info("=" * 50)
            log.info("上传订单列表...")
            log.info("=" * 50)
            upload_to_feishu(list_csv, "order_list")
        else:
            log.warning(f"文件不存在，跳过: {list_csv}")

        # 上传订单利润
        profit_csv = feishu_ready_dir / "order_profit_ready.csv"
        if profit_csv.exists():
            log.info("=" * 50)
            log.info("上传订单利润...")
            log.info("=" * 50)
            upload_to_feishu(profit_csv, "order_profit")
        else:
            log.warning(f"文件不存在，跳过: {profit_csv}")

        log.info("=" * 50)
        log.info("✅ 上传完成！")
        log.info("=" * 50)

    except Exception:
        log.exception("上传失败")
        raise


if __name__ == "__main__":
    main()
