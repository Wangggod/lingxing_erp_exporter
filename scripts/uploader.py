"""飞书上传模块：将数据上传到飞书电子表格"""

import pandas as pd
from pathlib import Path
import json
import requests
import lark_oapi as lark
from lark_oapi.api.sheets.v3 import *
from scripts.feishu_helper import load_feishu_config, get_client
from scripts.logger import setup_logger

log = setup_logger()


def get_tenant_access_token(app_id: str, app_secret: str) -> str:
    """获取 tenant_access_token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {
        "app_id": app_id,
        "app_secret": app_secret
    }

    response = requests.post(url, headers=headers, json=data)
    result = response.json()

    if result.get("code") != 0:
        raise Exception(f"获取 token 失败: {result}")

    return result["tenant_access_token"]


def read_sheet_data(
    spreadsheet_token: str,
    sheet_id: str,
    range_notation: str = None,
    access_token: str = None
) -> list[list]:
    """
    读取飞书表格数据（使用 HTTP API）。

    Args:
        spreadsheet_token: 电子表格 token
        sheet_id: sheet ID
        range_notation: 读取范围，如 "A1:Z1000"，默认读取全部
        access_token: 访问令牌

    Returns:
        二维列表，每行是一个列表
    """
    # 如果没有指定范围，读取所有数据（最多2000行）
    if range_notation is None:
        range_notation = f"{sheet_id}!A1:ZZ2000"

    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_notation}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    result = response.json()

    if result.get("code") != 0:
        raise Exception(f"读取表格数据失败: {result}")

    # 提取数据
    data = result.get("data", {}).get("values", [])
    return data


def check_duplicate_order_list(
    existing_data: list[list],
    new_df: pd.DataFrame,
    id_column: str = "防重复编号"
) -> pd.DataFrame:
    """
    检查订单列表的重复数据（基于订单号）。

    Args:
        existing_data: 飞书表格中已有的数据
        new_df: 新的数据 DataFrame
        id_column: 用于去重的列名

    Returns:
        去重后的 DataFrame
    """
    if not existing_data or len(existing_data) < 2:
        # 表格为空或只有表头，直接返回所有数据
        log.info("表格为空，无需去重")
        return new_df

    # 提取已有的订单号（第一列）
    existing_ids = set()
    for row in existing_data[1:]:  # 跳过表头
        if row and len(row) > 0:
            existing_ids.add(str(row[0]))

    log.info(f"表格中已有 {len(existing_ids)} 个订单号")

    # 筛选出不重复的数据
    original_count = len(new_df)
    new_df_filtered = new_df[~new_df[id_column].astype(str).isin(existing_ids)]
    filtered_count = len(new_df_filtered)

    log.info(f"去重结果: {original_count} 行 -> {filtered_count} 行（新增 {filtered_count} 行）")

    return new_df_filtered


def check_duplicate_order_profit(
    existing_data: list[list],
    new_df: pd.DataFrame,
    date_column: str = "站点日期"
) -> pd.DataFrame:
    """
    检查订单利润的重复数据（基于站点日期）。

    Args:
        existing_data: 飞书表格中已有的数据
        new_df: 新的数据 DataFrame
        date_column: 日期列名

    Returns:
        去重后的 DataFrame
    """
    if not existing_data or len(existing_data) < 2:
        log.info("表格为空，无需去重")
        return new_df

    # 提取已有的日期（第二列）
    existing_dates = set()
    for row in existing_data[1:]:  # 跳过表头
        if row and len(row) > 1:
            existing_dates.add(str(row[1]))

    log.info(f"表格中已有的日期: {existing_dates}")

    # 检查新数据的日期
    new_date = new_df[date_column].iloc[0] if len(new_df) > 0 else None

    if new_date and str(new_date) in existing_dates:
        log.warning(f"站点日期 {new_date} 的数据已存在，跳过上传")
        return pd.DataFrame()  # 返回空 DataFrame

    log.info(f"站点日期 {new_date} 是新数据，可以上传")
    return new_df


def append_to_sheet(
    spreadsheet_token: str,
    sheet_id: str,
    data: list[list],
    access_token: str
) -> bool:
    """
    追加数据到飞书表格末端（使用 HTTP API）。

    Args:
        spreadsheet_token: 电子表格 token
        sheet_id: sheet ID
        data: 要追加的数据（二维列表）
        access_token: 访问令牌

    Returns:
        是否成功
    """
    if not data:
        log.warning("没有数据需要上传")
        return False

    # 使用 append API（不需要指定 range，API 会自动追加到末尾）
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_append"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "valueRange": {
            "range": sheet_id,  # 只需要 sheet_id，不需要具体范围
            "values": data
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    result = response.json()

    if result.get("code") != 0:
        log.error(f"追加数据失败: {result}")
        raise Exception(f"追加数据失败: {result.get('msg', 'Unknown error')}")

    log.info(f"成功追加 {len(data)} 行数据")
    return True


def upload_to_feishu(
    csv_path: Path,
    sheet_type: str,
    config: dict = None
) -> bool:
    """
    上传 CSV 数据到飞书表格。

    Args:
        csv_path: CSV 文件路径
        sheet_type: 表格类型（"order_list" 或 "order_profit"）
        config: 飞书配置

    Returns:
        是否成功
    """
    if config is None:
        config = load_feishu_config()

    # 读取 CSV
    df = pd.read_csv(csv_path)
    log.info(f"读取 CSV: {csv_path}，共 {len(df)} 行")

    # 获取配置
    app_id = config["app_id"]
    app_secret = config["app_secret"]
    spreadsheet_token = config["spreadsheet_token"]
    sheet_config = config["sheets"][sheet_type]
    sheet_id = sheet_config["sheet_id"]
    sheet_name = sheet_config["name"]

    log.info(f"准备上传到: {sheet_name} (sheet_id: {sheet_id})")

    # 获取访问令牌
    access_token = get_tenant_access_token(app_id, app_secret)

    # 读取现有数据（用于去重）
    try:
        existing_data = read_sheet_data(spreadsheet_token, sheet_id, access_token=access_token)
        log.info(f"读取到现有数据: {len(existing_data)} 行")
    except Exception as e:
        log.warning(f"读取现有数据失败，假设表格为空: {e}")
        existing_data = []

    # 去重
    if sheet_type == "order_list":
        df_filtered = check_duplicate_order_list(existing_data, df)
    elif sheet_type == "order_profit":
        df_filtered = check_duplicate_order_profit(existing_data, df)
    else:
        log.error(f"未知的表格类型: {sheet_type}")
        return False

    if len(df_filtered) == 0:
        log.info("没有新数据需要上传")
        return True

    # 转换为二维列表（不包含表头，因为表头已经在飞书表格中）
    # 处理 NaN 值，替换为空字符串
    df_filtered = df_filtered.fillna("")

    # 获取列名，识别哪些是数字列
    numeric_columns = df_filtered.select_dtypes(include=['int64', 'float64']).columns.tolist()

    # 转换为列表，保持数据类型
    data_to_upload = []
    for _, row in df_filtered.iterrows():
        row_data = []
        for col_name, value in row.items():
            # 空值
            if value == "":
                row_data.append("")
            # 前两列（防重复编号和站点日期）强制为字符串
            elif col_name in ["防重复编号", "站点日期"]:
                row_data.append(str(value))
            # 数字列保持数字类型
            elif col_name in numeric_columns:
                if isinstance(value, float) and value.is_integer():
                    row_data.append(int(value))
                else:
                    row_data.append(value)
            # 其他列转为字符串
            else:
                row_data.append(str(value))
        data_to_upload.append(row_data)

    # 上传
    success = append_to_sheet(spreadsheet_token, sheet_id, data_to_upload, access_token)

    if success:
        log.info(f"✅ 成功上传 {len(data_to_upload)} 行到 {sheet_name}")
    else:
        log.error(f"❌ 上传失败")

    return success
