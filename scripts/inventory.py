"""FBA库存处理模块：活跃店铺过滤 + FNSKU去重 + 库龄聚合"""

import pandas as pd
from pathlib import Path
from scripts.logger import setup_logger

log = setup_logger()

_COUNTRY_KEYWORDS = [
    ('US', '美国'), ('美国', '美国'), ('北美', '美国'),
    ('CA', '加拿大'), ('加拿大', '加拿大'),
    ('MX', '墨西哥'), ('墨西哥', '墨西哥'),
    ('UK', '英国'), ('英国', '英国'),
    ('DE', '德国'), ('德国', '德国'),
    ('FR', '法国'), ('法国', '法国'),
    ('IT', '意大利'), ('意大利', '意大利'),
    ('ES', '西班牙'), ('西班牙', '西班牙'),
    ('BR', '巴西'), ('巴西', '巴西'),
    ('NL', '荷兰'), ('荷兰', '荷兰'),
    ('SE', '瑞典'), ('瑞典', '瑞典'),
    ('PL', '波兰'), ('波兰', '波兰'),
    ('BE', '比利时'), ('比利时', '比利时'),
    ('IE', '爱尔兰'), ('爱尔兰', '爱尔兰'),
]


def _active_store_set(merchant_df: pd.DataFrame) -> set:
    """从店铺授权表提取活跃店铺名集合。"""
    return set(merchant_df[merchant_df['店铺授权'] == '授权正常']['店铺'].unique())


def _is_active_store(store_str, active_stores: set) -> bool:
    """判断店铺是否活跃（支持逗号拼接的多店铺名）。"""
    if pd.isna(store_str):
        return False
    return any(s.strip() in active_stores for s in str(store_str).split(','))


def _extract_country(warehouse: str) -> str:
    """从所属仓库名提取国家。"""
    wh = str(warehouse)
    for keyword, country in _COUNTRY_KEYWORDS:
        if keyword in wh:
            return country
    return '其他'


def _warehouse_priority(warehouse: str) -> int:
    """仓库优先级：US/美国/北美 = 0（最高），其他 = 1。"""
    wh = str(warehouse)
    if 'US' in wh or '美国' in wh or '北美' in wh:
        return 0
    return 1


def aggregate_inventory(
    fba_inventory_path: Path,
    merchant_list_path: Path,
    product_names: list[str],
) -> pd.DataFrame:
    """
    处理 FBA 库存数据：过滤活跃店铺 → 筛选品名 → FNSKU去重 → 聚合到国家×品名。

    Args:
        fba_inventory_path: fba_inventory.xlsx 路径
        merchant_list_path: merchant_list.xlsx 路径
        product_names: 品名列表

    Returns:
        聚合后的 DataFrame，index 为 (国家, 品名)，
        列：FBA可售, FBA待调仓, FBA调仓中, 库龄_90天内, 库龄_91到180天, 库龄_181到365天, 库龄_超365天
    """
    # 检测损坏文件（领星偶尔返回错误 JSON 而非 xlsx）
    file_size = fba_inventory_path.stat().st_size
    if file_size < 1024:
        raw = fba_inventory_path.read_bytes()
        if raw.startswith(b'{') or raw.startswith(b'<'):
            log.warning(f"FBA库存文件损坏（{file_size}B），跳过库存聚合")
            return pd.DataFrame()

    inv = pd.read_excel(fba_inventory_path)
    merchant = pd.read_excel(merchant_list_path)
    log.info(f"读取FBA库存: {len(inv)} 行, 店铺列表: {len(merchant)} 行")

    # 1. 活跃店铺过滤
    active_stores = _active_store_set(merchant)
    inv = inv[inv['店铺'].apply(lambda s: _is_active_store(s, active_stores))]
    log.info(f"活跃店铺过滤后: {len(inv)} 行")

    # 2. 按品名筛选
    inv = inv[inv['品名'].isin(product_names)]
    log.info(f"品名筛选后: {len(inv)} 行 (品名={product_names})")

    if inv.empty:
        return pd.DataFrame()

    # 3. FNSKU 去重：优先保留 US 仓
    inv = inv.copy()
    inv['_priority'] = inv['所属仓库'].apply(_warehouse_priority)
    inv = inv.sort_values('_priority').drop_duplicates(subset='FNSKU', keep='first')
    inv = inv.drop(columns='_priority')
    log.info(f"FNSKU去重后: {len(inv)} 行")

    # 4. 提取国家 + 合并库龄为 4 档
    inv['国家'] = inv['所属仓库'].apply(_extract_country)
    inv['库龄_90天内'] = inv['30天内库龄'] + inv['31-60天库龄'] + inv['61-90天库龄']
    inv['库龄_91到180天'] = inv['91-180天库龄']
    inv['库龄_181到365天'] = inv['181-270天库龄'] + inv['271-330天库龄'] + inv['331-365天库龄']
    inv['库龄_超365天'] = inv['大于365天库龄']

    # 5. 聚合到 国家×品名
    agg_result = inv.groupby(['国家', '品名']).agg({
        'FBA可售': 'sum',
        'FBA待调仓': 'sum',
        'FBA调仓中': 'sum',
        '库龄_90天内': 'sum',
        '库龄_91到180天': 'sum',
        '库龄_181到365天': 'sum',
        '库龄_超365天': 'sum',
    })

    log.info(f"库存聚合完成: {len(agg_result)} 行")
    return agg_result
