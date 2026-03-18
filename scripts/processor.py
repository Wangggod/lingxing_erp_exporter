"""数据处理模块：筛选特定产品组的数据"""

import pandas as pd
from pathlib import Path
from scripts.logger import setup_logger

log = setup_logger()


def filter_by_product(
    raw_date_dir: Path,
    processed_date_dir: Path,
    group_name: str,
    product_names: list[str],
    source_files: dict[str, str] = None
) -> dict[str, Path]:
    """
    根据品名列表筛选数据并保存为 CSV。

    Args:
        raw_date_dir: 原始数据日期目录 (如 data/raw/2026-02-23)
        processed_date_dir: 处理后数据日期目录 (如 data/processed/2026-02-23)
        group_name: 项目组名称 (如 "保险箱")
        product_names: 品名列表 (如 ["艾洛克保险箱TX9S", "艾洛克保险箱WX001"])
        source_files: 源文件名映射

    Returns:
        保存的 CSV 文件路径字典
    """
    if source_files is None:
        source_files = {
            "profit": "order_profit.xlsx",
            "list": "order_list.xlsx",
            "performance": "product_performance.xlsx"
        }

    # 在 processed 目录下创建项目组子目录
    product_dir = processed_date_dir / group_name
    product_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"创建项目组目录: {product_dir}")

    saved_files = {}

    for file_type, filename in source_files.items():
        source_path = raw_date_dir / filename

        if not source_path.exists():
            log.warning(f"源文件不存在，跳过: {source_path}")
            continue

        # 读取 Excel
        log.info(f"读取文件: {source_path}")
        df = pd.read_excel(source_path)

        # 检查是否有"品名"列
        if "品名" not in df.columns:
            log.error(f"文件中没有'品名'列，跳过: {source_path}")
            continue

        # 筛选数据（支持多品名）
        original_count = len(df)
        filtered_df = df[df["品名"].isin(product_names)]
        filtered_count = len(filtered_df)

        log.info(f"筛选结果: {original_count} 行 -> {filtered_count} 行 (项目组='{group_name}', 品名={product_names})")

        if filtered_count == 0:
            log.warning(f"未找到品名为 {product_names} 的数据")

        # 保存为 CSV
        output_filename = filename.replace(".xlsx", ".csv")
        output_path = product_dir / output_filename
        filtered_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log.info(f"已保存: {output_path}")

        saved_files[file_type] = output_path

    return saved_files


def process_date(
    date_str: str,
    group_name: str,
    product_names: list[str],
    raw_base_dir: Path = None,
    processed_base_dir: Path = None
) -> dict[str, Path]:
    """
    处理指定日期的数据。

    Args:
        date_str: 日期字符串 (如 "2026-02-23")
        group_name: 项目组名称
        product_names: 品名列表
        raw_base_dir: 原始数据根目录，默认为 data/raw
        processed_base_dir: 处理后数据根目录，默认为 data/processed

    Returns:
        保存的 CSV 文件路径字典
    """
    root = Path(__file__).resolve().parent.parent / "data"

    if raw_base_dir is None:
        raw_base_dir = root / "raw"
    if processed_base_dir is None:
        processed_base_dir = root / "processed"

    raw_date_dir = raw_base_dir / date_str
    processed_date_dir = processed_base_dir / date_str

    if not raw_date_dir.exists():
        raise FileNotFoundError(f"原始数据目录不存在: {raw_date_dir}")

    log.info(f"开始处理日期: {date_str}, 项目组: {group_name}")
    return filter_by_product(raw_date_dir, processed_date_dir, group_name, product_names)
