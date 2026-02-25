"""数据预处理：添加防重复编号和站点日期，为上传飞书做准备"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from scripts.logger import setup_logger

log = setup_logger()


def format_date_for_feishu(date_str: str) -> str:
    """
    将日期格式从 YYYY-MM-DD 转换为标准 ISO 格式（保持 YYYY-MM-DD）。

    Args:
        date_str: 日期字符串，如 "2026-02-23"

    Returns:
        ISO 格式日期，如 "2026-02-23"
    """
    # 直接返回 ISO 格式，这是最标准的日期格式
    # 飞书多维表格能正确识别这个格式
    return date_str


def preprocess_order_list(csv_path: Path, date_str: str) -> pd.DataFrame:
    """
    预处理订单列表数据。

    处理步骤：
    1. 使用"订单号"作为防重复编号（移到第一列）
    2. 添加"站点日期"（第二列）
    3. 保持其他列顺序不变

    Args:
        csv_path: CSV 文件路径
        date_str: 日期字符串，如 "2026-02-23"

    Returns:
        预处理后的 DataFrame
    """
    df = pd.read_csv(csv_path)

    # 检查是否有"订单号"列
    if "订单号" not in df.columns:
        raise ValueError(f"CSV 文件中没有'订单号'列: {csv_path}")

    # 添加站点日期
    feishu_date = format_date_for_feishu(date_str)
    df.insert(0, "站点日期", feishu_date)

    # 将"订单号"移到第一列作为防重复编号
    order_id_col = df.pop("订单号")
    df.insert(0, "防重复编号", order_id_col)

    log.info(f"[订单列表] 预处理完成: {len(df)} 行")
    return df


def preprocess_order_profit(csv_path: Path, date_str: str) -> pd.DataFrame:
    """
    预处理订单利润数据。

    处理步骤：
    1. 生成序号作为防重复编号（格式：20260223_001）
    2. 添加"站点日期"（第二列）
    3. 保持其他列顺序不变

    Args:
        csv_path: CSV 文件路径
        date_str: 日期字符串，如 "2026-02-23"

    Returns:
        预处理后的 DataFrame
    """
    df = pd.read_csv(csv_path)

    # 生成防重复编号：日期序号（如 20260223_001）
    date_prefix = date_str.replace("-", "")
    df["防重复编号"] = [f"{date_prefix}_{i+1:03d}" for i in range(len(df))]

    # 添加站点日期
    feishu_date = format_date_for_feishu(date_str)
    df["站点日期"] = feishu_date

    # 调整列顺序：防重复编号在第一列，站点日期在第二列
    cols = df.columns.tolist()
    cols.remove("防重复编号")
    cols.remove("站点日期")
    df = df[["防重复编号", "站点日期"] + cols]

    log.info(f"[订单利润] 预处理完成: {len(df)} 行，编号范围 {date_prefix}_001 ~ {date_prefix}_{len(df):03d}")
    return df


def preprocess_product_data(
    product_dir: Path,
    date_str: str,
    output_dir: Path = None
) -> dict[str, Path]:
    """
    预处理产品数据，为上传飞书做准备。

    Args:
        product_dir: 产品数据目录（如 data/processed/2026-02-23/半开猫砂盆）
        date_str: 日期字符串（如 "2026-02-23"）
        output_dir: 输出目录，默认为 product_dir 的父目录下的 feishu-ready 目录

    Returns:
        保存的文件路径字典
    """
    if output_dir is None:
        # 输出到 data/processed/2026-02-23/feishu-ready/半开猫砂盆/
        output_dir = product_dir.parent / "feishu-ready" / product_dir.name

    output_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"预处理数据，输出目录: {output_dir}")

    saved_files = {}

    # 处理订单列表
    list_csv = product_dir / "order_list.csv"
    if list_csv.exists():
        df_list = preprocess_order_list(list_csv, date_str)
        output_path = output_dir / "order_list_ready.csv"
        df_list.to_csv(output_path, index=False, encoding="utf-8-sig")
        log.info(f"已保存: {output_path}")
        saved_files["list"] = output_path
    else:
        log.warning(f"文件不存在，跳过: {list_csv}")

    # 处理订单利润
    profit_csv = product_dir / "order_profit.csv"
    if profit_csv.exists():
        df_profit = preprocess_order_profit(profit_csv, date_str)
        output_path = output_dir / "order_profit_ready.csv"
        df_profit.to_csv(output_path, index=False, encoding="utf-8-sig")
        log.info(f"已保存: {output_path}")
        saved_files["profit"] = output_path
    else:
        log.warning(f"文件不存在，跳过: {profit_csv}")

    return saved_files
