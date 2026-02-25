"""数据聚合模块：将订单列表和订单利润聚合为汇总表"""

import pandas as pd
from pathlib import Path
from scripts.logger import setup_logger

log = setup_logger()


def aggregate_daily_data(
    order_list_csv: Path,
    order_profit_csv: Path,
    output_path: Path = None
) -> pd.DataFrame:
    """
    聚合每日数据，按日期-国家汇总。

    Args:
        order_list_csv: 订单列表 CSV 路径
        order_profit_csv: 订单利润 CSV 路径
        output_path: 输出路径（可选）

    Returns:
        聚合后的 DataFrame
    """
    # 读取数据
    list_df = pd.read_csv(order_list_csv)
    profit_df = pd.read_csv(order_profit_csv)

    log.info(f"读取订单列表: {len(list_df)} 行")
    log.info(f"读取订单利润: {len(profit_df)} 行")

    # ========== 从 order_list 聚合 ==========

    # 过滤条件：订单状态不为 Canceled，且换货订单不为"是"
    valid_orders = list_df[
        (list_df['订单状态'] != 'Canceled') &
        (list_df['换货订单'] != '是')
    ].copy()

    log.info(f"有效订单（排除 Canceled 和换货）: {len(valid_orders)} 行")

    # 按日期-国家分组聚合
    list_agg = valid_orders.groupby(['站点日期', '国家']).agg({
        '订单币种': 'first',  # 货币（同一国家应该一致）
        '数量': 'sum',  # 总销量
    }).rename(columns={
        '订单币种': '货币',
        '数量': '总销量'
    })

    # FBM订单（订单类型 = MFN）
    fbm_orders = valid_orders[valid_orders['订单类型'] == 'MFN'].groupby(['站点日期', '国家'])['数量'].sum()
    list_agg['FBM订单'] = fbm_orders

    # FBA订单（订单类型 = AFN）
    fba_orders = valid_orders[valid_orders['订单类型'] == 'AFN'].groupby(['站点日期', '国家'])['数量'].sum()
    list_agg['FBA订单'] = fba_orders

    # 总销售额（单价求和，排除 Canceled、换货和退货）
    valid_sales = list_df[
        (list_df['订单状态'] != 'Canceled') &
        (list_df['换货订单'] != '是') &
        (list_df['是否退货'] != '是')
    ].copy()
    total_sales = valid_sales.groupby(['站点日期', '国家'])['单价'].sum()
    list_agg['总销售额'] = total_sales

    # 优惠券订单数（促销编码不为空，排除 Canceled、换货和退货）
    coupon_orders = valid_sales[valid_sales['促销编码'].notna() & (valid_sales['促销编码'] != '')].groupby(['站点日期', '国家']).size()
    list_agg['优惠券订单数'] = coupon_orders

    # 填充缺失值为 0
    list_agg['FBM订单'] = list_agg['FBM订单'].fillna(0).astype(int)
    list_agg['FBA订单'] = list_agg['FBA订单'].fillna(0).astype(int)
    list_agg['优惠券订单数'] = list_agg['优惠券订单数'].fillna(0).astype(int)

    # ========== 从 order_profit 聚合 ==========

    profit_agg = profit_df.groupby(['站点日期', '国家']).agg({
        '币种': 'first',  # 货币（用于填补 list 中缺失的货币）
        '广告销量': 'sum',  # 广告单
        '广告花费': lambda x: abs(x).sum(),  # 总广告花费（绝对值）
        '退款量': 'sum',  # 今日退款数量
        '退款金额': lambda x: abs(x).sum()  # 今日退款金额（绝对值）
    }).rename(columns={
        '币种': '货币_profit',
        '广告销量': '广告单',
        '广告花费': '总广告花费',
        '退款量': '今日退款数量',
        '退款金额': '今日退款金额'
    })

    # ========== 合并 ==========

    result = list_agg.join(profit_agg, how='outer')

    # 合并货币列（优先使用 list 的，如果为空则使用 profit 的）
    result['货币'] = result['货币'].fillna(result['货币_profit'])
    result = result.drop(columns=['货币_profit'])

    # 填充缺失值
    result['广告单'] = result['广告单'].fillna(0).astype(int)
    result['总广告花费'] = result['总广告花费'].fillna(0)
    result['今日退款数量'] = result['今日退款数量'].fillna(0).astype(int)
    result['今日退款金额'] = result['今日退款金额'].fillna(0)

    # 填充其他数字列的缺失值为 0
    result['总销量'] = result['总销量'].fillna(0).astype(int)
    result['FBM订单'] = result['FBM订单'].fillna(0).astype(int)
    result['FBA订单'] = result['FBA订单'].fillna(0).astype(int)
    result['总销售额'] = result['总销售额'].fillna(0)
    result['优惠券订单数'] = result['优惠券订单数'].fillna(0).astype(int)

    # 重置索引
    result = result.reset_index()

    # 调整列顺序
    columns_order = [
        '站点日期', '国家', '货币',
        '总销量', 'FBM订单', 'FBA订单', '广告单',
        '总销售额', '优惠券订单数', '总广告花费',
        '今日退款数量', '今日退款金额'
    ]
    result = result[columns_order]

    log.info(f"聚合完成: {len(result)} 行")
    log.info(f"聚合维度: {result[['站点日期', '国家']].to_string(index=False)}")

    # 保存
    if output_path:
        result.to_csv(output_path, index=False, encoding='utf-8-sig')
        log.info(f"已保存到: {output_path}")

    return result


def aggregate_product_data(
    product_dir: Path,
    date_str: str,
    output_dir: Path = None
) -> Path:
    """
    聚合产品数据。

    Args:
        product_dir: 产品数据目录（如 data/processed/2026-02-23/feishu-ready/半开猫砂盆）
        date_str: 日期字符串
        output_dir: 输出目录，默认为 product_dir

    Returns:
        聚合后的文件路径
    """
    if output_dir is None:
        output_dir = product_dir

    # 输入文件
    order_list_csv = product_dir / "order_list_ready.csv"
    order_profit_csv = product_dir / "order_profit_ready.csv"

    if not order_list_csv.exists():
        raise FileNotFoundError(f"文件不存在: {order_list_csv}")
    if not order_profit_csv.exists():
        raise FileNotFoundError(f"文件不存在: {order_profit_csv}")

    # 输出文件
    output_path = output_dir / "daily_summary.csv"

    # 聚合
    log.info("=" * 60)
    log.info(f"开始聚合数据 - 日期: {date_str}")
    log.info("=" * 60)

    result_df = aggregate_daily_data(order_list_csv, order_profit_csv, output_path)

    log.info("=" * 60)
    log.info("聚合结果预览:")
    log.info("=" * 60)
    log.info("\n" + result_df.to_string(index=False))

    return output_path
