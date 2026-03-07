"""数据聚合模块：将订单列表和订单利润聚合为汇总表"""

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from scripts.logger import setup_logger
from scripts.fbm_rates import get_estimated_shipping

log = setup_logger()


def get_coupon_face_value(
    promo_code: str,
    current_df: pd.DataFrame,
    product_dir: Path,
    date_str: str,
    lookback_days: int = 4
) -> float:
    """
    查找某个促销编码对应的优惠券面额。

    优先从当天数据中找 Shipped 订单，找不到则往前查最多 lookback_days 天。
    Shipped 订单的 促销费-商品折扣 即为该优惠券的真实面额。

    Returns:
        优惠券面额，找不到时返回 0.0
    """
    def find_in_df(df: pd.DataFrame) -> float:
        shipped = df[
            (df['促销编码'] == promo_code) &
            (df['订单状态'] == 'Shipped') &
            (df['促销费-商品折扣'] > 0)
        ]['促销费-商品折扣']
        return float(shipped.iloc[0]) if len(shipped) > 0 else 0.0

    # 先在当天数据中找
    value = find_in_df(current_df)
    if value > 0:
        return value

    # 往历史查找：product_dir = data/processed/{date}/feishu-ready/{product}
    processed_base = product_dir.parent.parent.parent
    product_name = product_dir.name
    date = datetime.strptime(date_str, '%Y-%m-%d')

    for i in range(1, lookback_days + 1):
        prev_date = (date - timedelta(days=i)).strftime('%Y-%m-%d')
        prev_path = processed_base / prev_date / "feishu-ready" / product_name / "order_list_ready.csv"
        if prev_path.exists():
            prev_df = pd.read_csv(prev_path)
            value = find_in_df(prev_df)
            if value > 0:
                log.info(f"优惠券 [{promo_code}] 面额 {value}，来自历史数据 {prev_date}")
                return value

    log.warning(f"优惠券 [{promo_code}] 未找到 Shipped 订单，面额按 0 处理")
    return 0.0


def get_fba_fee_per_unit(
    msku: str,
    current_df: pd.DataFrame,
    product_dir: Path,
    date_str: str,
    lookback_days: int = 4
) -> float:
    """
    查找某个 MSKU 对应的 FBA 费用（每件）。

    从当天 + 历史 Shipped AFN 订单收集所有非零 FBA 费，取中位数以过滤异常值。

    Returns:
        每件 FBA 费用（正数），找不到时返回 0.0
    """
    def collect_from_df(df: pd.DataFrame) -> list:
        """收集所有匹配的单件 FBA 费用"""
        shipped = df[
            (df['MSKU'] == msku) &
            (df['订单类型'] == 'AFN') &
            (df['订单状态'] == 'Shipped') &
            (df['FBA费'] != 0)
        ]
        values = []
        for _, row in shipped.iterrows():
            qty = max(int(row['数量']), 1)
            values.append(abs(float(row['FBA费'])) / qty)
        return values

    # 收集当天所有值
    all_values = collect_from_df(current_df)

    # 往历史查找补充数据
    processed_base = product_dir.parent.parent.parent
    product_name = product_dir.name
    date = datetime.strptime(date_str, '%Y-%m-%d')

    for i in range(1, lookback_days + 1):
        prev_date = (date - timedelta(days=i)).strftime('%Y-%m-%d')
        prev_path = processed_base / prev_date / "feishu-ready" / product_name / "order_list_ready.csv"
        if prev_path.exists():
            prev_df = pd.read_csv(prev_path)
            all_values.extend(collect_from_df(prev_df))

    if all_values:
        median = sorted(all_values)[len(all_values) // 2]
        log.info(f"MSKU [{msku}] FBA费/件中位数 {median:.2f}（共 {len(all_values)} 个数据点）")
        return median

    log.warning(f"MSKU [{msku}] 未找到 Shipped AFN 订单，FBA费按 0 处理")
    return 0.0


def get_commission_per_unit(
    msku: str,
    order_type: str,
    current_df: pd.DataFrame,
    product_dir: Path,
    date_str: str,
    lookback_days: int = 4
) -> float:
    """
    查找某个 MSKU 对应的单件平台佣金。

    从当天 + 历史 Shipped 订单收集所有非零平台费，取中位数以过滤异常值。
    同时校验中位数佣金率不超过 15%（超过则警告）。

    Returns:
        单件平台佣金（正数），找不到时返回 0.0
    """
    def collect_from_df(df: pd.DataFrame) -> list:
        """收集所有匹配的单件佣金及单价"""
        shipped = df[
            (df['MSKU'] == msku) &
            (df['订单类型'] == order_type) &
            (df['订单状态'] == 'Shipped') &
            (df['平台费'] != 0)
        ]
        values = []
        for _, row in shipped.iterrows():
            qty = max(int(row['数量']), 1)
            per_unit = abs(float(row['平台费'])) / qty
            values.append((per_unit, float(row['单价'])))
        return values

    # 收集当天所有值
    all_values = collect_from_df(current_df)

    # 往历史查找补充数据
    processed_base = product_dir.parent.parent.parent
    product_name = product_dir.name
    date = datetime.strptime(date_str, '%Y-%m-%d')

    for i in range(1, lookback_days + 1):
        prev_date = (date - timedelta(days=i)).strftime('%Y-%m-%d')
        prev_path = processed_base / prev_date / "feishu-ready" / product_name / "order_list_ready.csv"
        if prev_path.exists():
            prev_df = pd.read_csv(prev_path)
            all_values.extend(collect_from_df(prev_df))

    if all_values:
        per_units = [v[0] for v in all_values]
        median = sorted(per_units)[len(per_units) // 2]
        # 异常校验：用中位数佣金率检查
        prices = [v[1] for v in all_values if v[1] > 0]
        if prices:
            median_price = sorted(prices)[len(prices) // 2]
            if median_price > 0:
                rate = median / median_price
                if rate > 0.15:
                    log.warning(
                        f"MSKU [{msku}] 佣金率异常 {rate:.2%}（超过15%），"
                        f"中位数佣金={median:.2f} 中位数单价={median_price:.2f}，请人工核查"
                    )
        log.info(f"MSKU [{msku}] 佣金/件中位数 {median:.2f}（共 {len(all_values)} 个数据点）")
        return median

    log.warning(f"MSKU [{msku}] 订单类型 [{order_type}] 未找到 Shipped 订单，平台佣金按 0 处理")
    return 0.0


def get_unit_cost(
    country: str,
    cost_field: str,
    current_profit_df: pd.DataFrame,
    product_dir: Path,
    date_str: str,
    lookback_days: int = 4
) -> float:
    """
    通用：从 profit 表获取单件成本（采购均价/头程均价），按国家取中位数回填。

    从当天 profit 表收集同国家、指定字段 > 0 的所有值，不够则往历史 lookback。

    Returns:
        单件成本中位数，找不到时返回 0.0
    """
    def collect_from_df(df: pd.DataFrame) -> list:
        """收集同国家所有非零值"""
        matched = df[
            (df['国家'] == country) &
            (df[cost_field].notna()) &
            (df[cost_field] > 0)
        ]
        return [float(v) for v in matched[cost_field]]

    # 收集当天所有值
    all_values = collect_from_df(current_profit_df)

    # 往历史查找补充数据
    processed_base = product_dir.parent.parent.parent
    product_name = product_dir.name
    date = datetime.strptime(date_str, '%Y-%m-%d')

    for i in range(1, lookback_days + 1):
        prev_date = (date - timedelta(days=i)).strftime('%Y-%m-%d')
        prev_path = processed_base / prev_date / "feishu-ready" / product_name / "order_profit_ready.csv"
        if prev_path.exists():
            prev_df = pd.read_csv(prev_path)
            all_values.extend(collect_from_df(prev_df))

    if all_values:
        median = sorted(all_values)[len(all_values) // 2]
        log.info(f"国家 [{country}] {cost_field}中位数 {median:.2f}（共 {len(all_values)} 个数据点）")
        return median

    log.warning(f"国家 [{country}] 未找到 {cost_field} > 0 的记录，按 0 处理")
    return 0.0


def aggregate_daily_data(
    order_list_csv: Path,
    order_profit_csv: Path,
    output_path: Path = None,
    date_str: str = None
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
    afn_orders = valid_orders[valid_orders['订单类型'] == 'AFN']
    fba_orders = afn_orders.groupby(['站点日期', '国家'])['数量'].sum()
    list_agg['FBA订单'] = fba_orders

    # 总FBA费：订单自带非零值则直接用（Pending 亦然），为 0 才去历史查
    product_dir = order_list_csv.parent
    fba_fee_map = {}
    if date_str and len(afn_orders) > 0:
        for (date, country), group in afn_orders.groupby(['站点日期', '国家']):
            total_fee = 0.0
            for _, row in group.iterrows():
                if row['FBA费'] != 0:
                    total_fee += abs(float(row['FBA费']))
                else:
                    fee_per_unit = get_fba_fee_per_unit(row['MSKU'], list_df, product_dir, date_str)
                    total_fee += fee_per_unit * int(row['数量'])
            fba_fee_map[(date, country)] = total_fee

    if fba_fee_map:
        idx = pd.MultiIndex.from_tuples(fba_fee_map.keys(), names=['站点日期', '国家'])
        list_agg['总FBA费'] = pd.Series(fba_fee_map.values(), index=idx)
    else:
        list_agg['总FBA费'] = 0.0

    # 总平台佣金：订单自带非零值则直接用（Pending 亦然），为 0 才去历史查
    commission_map = {}
    if date_str and len(valid_orders) > 0:
        for (date, country), group in valid_orders.groupby(['站点日期', '国家']):
            total_commission = 0.0
            for _, row in group.iterrows():
                if row['平台费'] != 0:
                    total_commission += abs(float(row['平台费']))
                else:
                    per_unit = get_commission_per_unit(
                        row['MSKU'], row['订单类型'], list_df, product_dir, date_str
                    )
                    total_commission += per_unit * int(row['数量'])
            commission_map[(date, country)] = total_commission

    if commission_map:
        idx = pd.MultiIndex.from_tuples(commission_map.keys(), names=['站点日期', '国家'])
        list_agg['总平台佣金'] = pd.Series(commission_map.values(), index=idx)
    else:
        list_agg['总平台佣金'] = 0.0

    # FBM运费预估：对每个 FBM 订单，按 国家+MSKU 查费率表取历史均值
    fbm_orders = valid_orders[valid_orders['订单类型'] == 'MFN']
    fbm_shipping_map = {}
    if len(fbm_orders) > 0:
        for (date, country), group in fbm_orders.groupby(['站点日期', '国家']):
            total_shipping = 0.0
            for _, row in group.iterrows():
                per_unit = get_estimated_shipping(country, row['MSKU'])
                total_shipping += per_unit * max(int(row['数量']), 1)
            fbm_shipping_map[(date, country)] = round(total_shipping, 2)

    if fbm_shipping_map:
        idx = pd.MultiIndex.from_tuples(fbm_shipping_map.keys(), names=['站点日期', '国家'])
        list_agg['FBM运费'] = pd.Series(fbm_shipping_map.values(), index=idx)
    else:
        list_agg['FBM运费'] = 0.0

    # 总销售额（单价求和，排除 Canceled、换货和退货）
    valid_sales = list_df[
        (list_df['订单状态'] != 'Canceled') &
        (list_df['换货订单'] != '是') &
        (list_df['是否退货'] != '是')
    ].copy()
    total_sales = valid_sales.groupby(['站点日期', '国家'])['单价'].sum()
    list_agg['总销售额'] = total_sales

    # 优惠券订单数（促销编码不为空，排除 Canceled、换货和退货）
    coupon_df = valid_sales[valid_sales['促销编码'].notna() & (valid_sales['促销编码'] != '')]
    coupon_orders = coupon_df.groupby(['站点日期', '国家']).size()
    list_agg['优惠券订单数'] = coupon_orders

    # 优惠券折扣总额：按 日期-国家 分组，再按促销编码算 次数 × 面额
    coupon_discount_map = {}
    if date_str and len(coupon_df) > 0:
        for (date, country), group in coupon_df.groupby(['站点日期', '国家']):
            total_discount = 0.0
            for promo_code, code_group in group.groupby('促销编码'):
                face_value = get_coupon_face_value(promo_code, list_df, product_dir, date_str)
                total_discount += len(code_group) * face_value
            coupon_discount_map[(date, country)] = total_discount

    if coupon_discount_map:
        idx = pd.MultiIndex.from_tuples(coupon_discount_map.keys(), names=['站点日期', '国家'])
        list_agg['优惠券折扣总额'] = pd.Series(coupon_discount_map.values(), index=idx)
    else:
        list_agg['优惠券折扣总额'] = 0.0

    # 填充缺失值为 0
    list_agg['FBM订单'] = list_agg['FBM订单'].fillna(0).astype(int)
    list_agg['FBA订单'] = list_agg['FBA订单'].fillna(0).astype(int)
    list_agg['总FBA费'] = list_agg['总FBA费'].fillna(0)
    list_agg['总平台佣金'] = list_agg['总平台佣金'].fillna(0)
    list_agg['优惠券订单数'] = list_agg['优惠券订单数'].fillna(0).astype(int)
    list_agg['优惠券折扣总额'] = list_agg['优惠券折扣总额'].fillna(0)
    list_agg['FBM运费'] = list_agg['FBM运费'].fillna(0)

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

    # ========== 采购成本 / 头程成本聚合 ==========

    purchase_cost_map = {}
    freight_cost_map = {}
    if date_str and len(profit_df) > 0:
        for (date, country), group in profit_df.groupby(['站点日期', '国家']):
            total_purchase = 0.0
            total_freight = 0.0
            for _, row in group.iterrows():
                qty = max(int(row['销量']), 0)
                if qty == 0:
                    continue

                # 采购均价
                purchase_unit = float(row['采购均价']) if row['采购均价'] > 0 else \
                    get_unit_cost(country, '采购均价', profit_df, product_dir, date_str)
                total_purchase += purchase_unit * qty

                # 头程均价
                freight_unit = float(row['头程均价']) if row['头程均价'] > 0 else \
                    get_unit_cost(country, '头程均价', profit_df, product_dir, date_str)
                total_freight += freight_unit * qty

            purchase_cost_map[(date, country)] = round(total_purchase, 2)
            freight_cost_map[(date, country)] = round(total_freight, 2)

    if purchase_cost_map:
        idx = pd.MultiIndex.from_tuples(purchase_cost_map.keys(), names=['站点日期', '国家'])
        profit_agg['总采购成本'] = pd.Series(purchase_cost_map.values(), index=idx)
    else:
        profit_agg['总采购成本'] = 0.0

    if freight_cost_map:
        idx = pd.MultiIndex.from_tuples(freight_cost_map.keys(), names=['站点日期', '国家'])
        profit_agg['总头程成本'] = pd.Series(freight_cost_map.values(), index=idx)
    else:
        profit_agg['总头程成本'] = 0.0

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
    result['优惠券折扣总额'] = result['优惠券折扣总额'].fillna(0)
    result['总FBA费'] = result['总FBA费'].fillna(0)
    result['总平台佣金'] = result['总平台佣金'].fillna(0)
    result['FBM运费'] = result['FBM运费'].fillna(0)
    result['总采购成本'] = result['总采购成本'].fillna(0)
    result['总头程成本'] = result['总头程成本'].fillna(0)

    # 实际销售额 = 总销售额 - 优惠券折扣总额
    result['实际销售额'] = result['总销售额'] - result['优惠券折扣总额']

    # 回款 = 实际销售额 - 广告 - 佣金 - FBA费 - FBM运费
    result['回款'] = (result['实际销售额'] - result['总广告花费']
                     - result['总平台佣金'] - result['总FBA费'] - result['FBM运费'])

    # 利润 = 回款 - 采购 - 头程
    result['利润'] = result['回款'] - result['总采购成本'] - result['总头程成本']

    # 重置索引
    result = result.reset_index()

    # 调整列顺序
    columns_order = [
        '站点日期', '国家', '货币',
        '总销量', 'FBM订单', 'FBA订单', '广告单',
        '总销售额', '优惠券订单数', '优惠券折扣总额', '实际销售额',
        '总平台佣金', '总FBA费', '总广告花费',
        '今日退款数量', '今日退款金额', 'FBM运费',
        '总采购成本', '总头程成本', '回款', '利润'
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

    result_df = aggregate_daily_data(order_list_csv, order_profit_csv, output_path, date_str=date_str)

    log.info("=" * 60)
    log.info("聚合结果预览:")
    log.info("=" * 60)
    log.info("\n" + result_df.to_string(index=False))

    return output_path
