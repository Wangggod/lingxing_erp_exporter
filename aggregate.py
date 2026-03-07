"""数据聚合入口：生成每日汇总数据"""

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.aggregator import aggregate_product_data
from scripts.fbm_rates import update_rates
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

    log.info(f"开始聚合数据 - 日期: {target_date}, 产品: {PRODUCT_NAME}")

    # 先用自发货订单数据更新 FBM 运费费率表
    RAW_DIR = ROOT / "data" / "raw"
    fbm_shipment_path = RAW_DIR / target_date / "fbm_shipment.xlsx"
    if fbm_shipment_path.exists():
        log.info("更新 FBM 运费费率表...")
        update_rates(fbm_shipment_path)
    else:
        log.info(f"无自发货订单文件 ({fbm_shipment_path})，跳过费率更新")

    # 预处理后的数据目录
    feishu_ready_dir = PROCESSED_DIR / target_date / "feishu-ready" / PRODUCT_NAME

    if not feishu_ready_dir.exists():
        log.error(f"数据目录不存在: {feishu_ready_dir}")
        log.info("请先运行 preprocess.py 生成预处理后的数据")
        return

    try:
        output_path = aggregate_product_data(feishu_ready_dir, target_date)
        log.info(f"\n✅ 聚合完成！文件保存在: {output_path}")

    except Exception:
        log.exception("数据聚合失败")
        raise


if __name__ == "__main__":
    main()
