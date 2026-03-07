"""FBM 运费费率表维护和查询模块

维护一个 JSON 文件记录历史 FBM 运费数据，
在聚合时根据历史均值预估 FBM 运费。
"""

import json
import pandas as pd
from pathlib import Path
from scripts.logger import setup_logger

log = setup_logger()

ROOT = Path(__file__).resolve().parent.parent
RATES_FILE = ROOT / "data" / "fbm_shipping_rates.json"

# 每个 国家|MSKU 组合最多保留的历史记录数
MAX_RECORDS = 20

# ── 列名映射（对应领星自发货订单导出 Excel）──────────────
COL_COUNTRY = "国家/地区"
COL_MSKU = "MSKU"
COL_SHIPPING_COST = "物流运费"
COL_SHIPPING_CURRENCY = "物流运费币种"
COL_ORDER_ID = "系统单号"
COL_ORDER_STATUS = "状态"
COL_DATE = "发货时间"
COL_QUANTITY = "数量"
COL_SITE = "站点"


def _load_rates() -> dict:
    """加载费率表 JSON 文件。"""
    if RATES_FILE.exists():
        with open(RATES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_rates(rates: dict) -> None:
    """保存费率表 JSON 文件。"""
    RATES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(RATES_FILE, "w", encoding="utf-8") as f:
        json.dump(rates, f, ensure_ascii=False, indent=2)


def update_rates(fbm_shipment_path: Path) -> int:
    """
    从自发货订单 Excel 中提取已发货订单的运费数据，更新费率表。

    Args:
        fbm_shipment_path: 自发货订单 Excel/CSV 文件路径

    Returns:
        新增记录数
    """
    if not fbm_shipment_path.exists():
        log.warning(f"自发货订单文件不存在，跳过费率更新: {fbm_shipment_path}")
        return 0

    # 读取数据
    suffix = fbm_shipment_path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        df = pd.read_excel(fbm_shipment_path)
    else:
        df = pd.read_csv(fbm_shipment_path)

    log.info(f"读取自发货订单: {len(df)} 行")

    # 检查必要列是否存在
    required_cols = [COL_COUNTRY, COL_MSKU, COL_SHIPPING_COST]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log.error(f"自发货订单缺少必要列: {missing}，可用列: {list(df.columns)}")
        return 0

    # 筛选有运费的订单（不按状态过滤，因为领星状态值为中文且运费在出库阶段就有）
    # 排除 CNY 运费（需要与订单币种一致，CNY 为头程成本不适合作为 FBM 运费）
    shipped = df[df[COL_SHIPPING_COST].notna() & (df[COL_SHIPPING_COST] > 0)].copy()
    if COL_SHIPPING_CURRENCY in df.columns:
        shipped = shipped[shipped[COL_SHIPPING_CURRENCY] != "CNY"]

    if len(shipped) == 0:
        log.info("没有有运费的订单，跳过费率更新")
        return 0

    # 加载现有费率表
    rates = _load_rates()
    added = 0

    for _, row in shipped.iterrows():
        country = str(row[COL_COUNTRY])
        msku = str(row[COL_MSKU])
        cost = float(row[COL_SHIPPING_COST])
        quantity = int(row[COL_QUANTITY]) if COL_QUANTITY in row.index and pd.notna(row.get(COL_QUANTITY)) else 1
        quantity = max(quantity, 1)

        # 计算单件运费
        cost_per_unit = cost / quantity

        key = f"{country}|{msku}"

        order_id = str(row[COL_ORDER_ID]) if COL_ORDER_ID in row.index and pd.notna(row.get(COL_ORDER_ID)) else ""
        date = str(row[COL_DATE]) if COL_DATE in row.index and pd.notna(row.get(COL_DATE)) else ""

        record = {
            "date": date,
            "cost": round(cost_per_unit, 2),
            "order_id": order_id,
        }

        if key not in rates:
            rates[key] = {"records": []}

        # 去重：同订单号不重复添加
        existing_ids = {r.get("order_id") for r in rates[key]["records"]}
        if order_id and order_id in existing_ids:
            continue

        rates[key]["records"].append(record)
        added += 1

        # 保留最近 MAX_RECORDS 条
        if len(rates[key]["records"]) > MAX_RECORDS:
            rates[key]["records"] = rates[key]["records"][-MAX_RECORDS:]

    _save_rates(rates)
    log.info(f"费率表更新完成: 新增 {added} 条记录，共 {len(rates)} 个 国家|MSKU 组合")
    return added


def get_estimated_shipping(country: str, msku: str, n: int = 5) -> float:
    """
    查询预估 FBM 单件运费。

    策略：
    1. 查找 {country}|{msku}，取最近 n 单的平均运费
    2. 若无记录，用同国家所有 MSKU 的平均值
    3. 再无则返回 0

    Args:
        country: 国家代码
        msku: MSKU
        n: 取最近几单计算平均值

    Returns:
        预估单件运费，找不到返回 0.0
    """
    rates = _load_rates()

    # 策略1：精确匹配 国家|MSKU
    key = f"{country}|{msku}"
    if key in rates and rates[key]["records"]:
        records = rates[key]["records"]
        recent = records[-n:]  # 取最近 n 条
        avg = sum(r["cost"] for r in recent) / len(recent)
        return round(avg, 2)

    # 策略2：回退到同国家所有 MSKU 的平均值
    country_prefix = f"{country}|"
    country_costs = []
    for k, v in rates.items():
        if k.startswith(country_prefix) and v["records"]:
            recent = v["records"][-n:]
            country_costs.extend(r["cost"] for r in recent)

    if country_costs:
        avg = sum(country_costs) / len(country_costs)
        log.info(f"MSKU [{msku}] 无运费记录，使用 {country} 国家均值: {avg:.2f}")
        return round(avg, 2)

    # 策略3：无数据
    log.warning(f"国家 [{country}] 无任何 FBM 运费记录，运费按 0 处理")
    return 0.0
