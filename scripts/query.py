"""数据查询模块：为 agent 和外部系统提供统一的数据查询接口

用法:
    # 自描述：查看可用字段、产品、日期范围（agent 应先调此命令）
    python -m scripts.query schema

    # 汇总查询
    python -m scripts.query summary --days 7 --fields 总销量,利润
    python -m scripts.query summary --days 30 --products 半开猫砂盆 --country 美国
    python -m scripts.query summary --start 2026-02-01 --end 2026-02-28

    # 明细查询
    python -m scripts.query detail --order 114-1607332-5352268
    python -m scripts.query detail --products 欧博尔面包机 --days 3 --status Shipped
    python -m scripts.query detail --products 欧博尔面包机 --days 7 --fields 防重复编号,订单状态,ASIN
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"
PRODUCTS_CONFIG = ROOT / "config" / "products.json"


def _load_products() -> dict:
    with open(PRODUCTS_CONFIG, encoding="utf-8") as f:
        return json.load(f)


def _available_dates() -> list[str]:
    """返回所有有 processed 数据的日期（升序）。"""
    return sorted([d.name for d in PROCESSED_DIR.iterdir() if d.is_dir() and d.name[:4].isdigit()])


def _resolve_date_range(
    days: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> list[str]:
    """根据参数计算日期列表。"""
    all_dates = _available_dates()
    if not all_dates:
        return []

    if start or end:
        s = start or all_dates[0]
        e = end or all_dates[-1]
        return [d for d in all_dates if s <= d <= e]

    if days:
        return all_dates[-days:]

    # 默认最近1天
    return all_dates[-1:]


# ==================== 汇总查询 ====================

def query_summary(
    days: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    products: Optional[list[str]] = None,
    country: Optional[str] = None,
    fields: Optional[list[str]] = None,
) -> dict:
    """
    查询汇总数据。

    Args:
        days: 最近 N 天
        start/end: 日期范围（YYYY-MM-DD）
        products: 产品名列表，None 则返回所有产品
        country: 按国家过滤
        fields: 只返回指定字段

    Returns:
        {"query": "summary", "date_range": [...], "data": {产品: [记录]}}
    """
    dates = _resolve_date_range(days, start, end)
    all_products = _load_products()
    target_products = products if products else list(all_products.keys())

    result_data = {}

    for date_str in dates:
        json_path = PROCESSED_DIR / date_str / "daily_summary.json"
        if not json_path.exists():
            continue

        with open(json_path, encoding="utf-8") as f:
            daily = json.load(f)

        for product_name in target_products:
            if product_name not in daily.get("products", {}):
                continue

            records = daily["products"][product_name]

            # 按国家过滤
            if country:
                records = [r for r in records if r.get("国家") == country]

            # 按字段过滤
            if fields:
                keep = ["站点日期", "国家"] + [f for f in fields if f not in ("站点日期", "国家")]
                records = [{k: r[k] for k in keep if k in r} for r in records]

            if records:
                result_data.setdefault(product_name, []).extend(records)

    return {
        "query": "summary",
        "date_range": [dates[0], dates[-1]] if dates else [],
        "data": result_data,
    }


# ==================== 明细查询 ====================

def query_detail(
    days: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    products: Optional[list[str]] = None,
    country: Optional[str] = None,
    fields: Optional[list[str]] = None,
    order: Optional[str] = None,
    status: Optional[str] = None,
) -> dict:
    """
    查询订单明细数据。

    Args:
        days: 最近 N 天
        start/end: 日期范围
        products: 产品名列表
        country: 按国家过滤
        fields: 只返回指定字段
        order: 按订单号精确查找
        status: 按订单状态过滤（如 Shipped, Pending）

    Returns:
        {"query": "detail", "date_range": [...], "data": {产品: [记录]}}
    """
    all_products = _load_products()
    target_products = products if products else list(all_products.keys())

    # 订单号查询：扫描所有日期和产品
    if order:
        dates = _resolve_date_range(days, start, end) if (days or start or end) else _available_dates()
    else:
        dates = _resolve_date_range(days, start, end)

    result_data = {}

    for date_str in dates:
        for product_name in target_products:
            csv_path = PROCESSED_DIR / date_str / "feishu-ready" / product_name / "order_list_ready.csv"
            if not csv_path.exists():
                continue

            df = pd.read_csv(csv_path)

            # 按订单号
            if order:
                df = df[df["防重复编号"].astype(str) == order]
                if df.empty:
                    continue

            # 按订单状态
            if status:
                df = df[df["订单状态"] == status]

            # 按国家
            if country:
                df = df[df["国家"] == country]

            if df.empty:
                continue

            # 按字段过滤
            if fields:
                keep = [f for f in fields if f in df.columns]
                df = df[keep]

            records = df.to_dict(orient="records")
            result_data.setdefault(product_name, []).extend(records)

        # 订单号查询找到就停
        if order and result_data:
            break

    return {
        "query": "detail",
        "date_range": [dates[0], dates[-1]] if dates else [],
        "data": result_data,
    }


# ==================== Schema ====================

def query_schema() -> dict:
    """
    返回查询模块的自描述信息：可用产品、日期范围、字段列表。

    字段列表从实际数据文件动态读取，新增聚合字段后自动可见，无需手动维护。

    Returns:
        {"products": [...], "date_range": [...], "summary_fields": [...], "detail_fields": [...]}
    """
    all_dates = _available_dates()
    products = list(_load_products().keys())

    # summary_fields: 从最新的 daily_summary.json 动态读取
    summary_fields = []
    for date_str in reversed(all_dates):
        json_path = PROCESSED_DIR / date_str / "daily_summary.json"
        if json_path.exists():
            with open(json_path, encoding="utf-8") as f:
                daily = json.load(f)
            for product_records in daily.get("products", {}).values():
                if product_records:
                    summary_fields = list(product_records[0].keys())
                    break
            if summary_fields:
                break

    # detail_fields: 从最新的 order_list_ready.csv 动态读取
    detail_fields = []
    for date_str in reversed(all_dates):
        for product_name in products:
            csv_path = PROCESSED_DIR / date_str / "feishu-ready" / product_name / "order_list_ready.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path, nrows=0)
                detail_fields = list(df.columns)
                break
        if detail_fields:
            break

    return {
        "products": products,
        "date_range": [all_dates[0], all_dates[-1]] if all_dates else [],
        "summary_fields": summary_fields,
        "detail_fields": detail_fields,
    }


# ==================== CLI ====================

def main():
    parser = argparse.ArgumentParser(description="数据查询工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # schema
    subparsers.add_parser("schema", help="查看可用字段、产品、日期范围")

    # 共享参数
    def add_common_args(p):
        p.add_argument("--days", type=int, help="最近 N 天")
        p.add_argument("--start", help="起始日期（YYYY-MM-DD）")
        p.add_argument("--end", help="结束日期（YYYY-MM-DD）")
        p.add_argument("--products", help="产品名（逗号分隔）")
        p.add_argument("--country", help="按国家过滤")
        p.add_argument("--fields", help="只返回指定字段（逗号分隔）")

    # summary
    summary_parser = subparsers.add_parser("summary", help="汇总查询")
    add_common_args(summary_parser)

    # detail
    detail_parser = subparsers.add_parser("detail", help="明细查询")
    add_common_args(detail_parser)
    detail_parser.add_argument("--order", help="按订单号查找")
    detail_parser.add_argument("--status", help="按订单状态过滤（如 Shipped, Pending）")

    args = parser.parse_args()

    if args.command == "schema":
        result = query_schema()
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
        print()
        return

    # 解析列表参数
    products = args.products.split(",") if args.products else None
    fields = args.fields.split(",") if args.fields else None

    if args.command == "summary":
        result = query_summary(
            days=args.days, start=args.start, end=args.end,
            products=products, country=args.country, fields=fields,
        )
    elif args.command == "detail":
        result = query_detail(
            days=args.days, start=args.start, end=args.end,
            products=products, country=args.country, fields=fields,
            order=args.order, status=args.status,
        )

    json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
    print()


if __name__ == "__main__":
    main()
