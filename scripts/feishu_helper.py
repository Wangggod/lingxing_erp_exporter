"""飞书 API 辅助工具"""

import json
from pathlib import Path
import lark_oapi as lark
from lark_oapi.api.sheets.v3 import *
from scripts.logger import setup_logger

log = setup_logger()


def load_feishu_config() -> dict:
    """加载飞书配置"""
    config_path = Path(__file__).resolve().parent.parent / "config" / "feishu.json"
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    return json.loads(config_path.read_text(encoding="utf-8"))


def get_client(config: dict = None) -> lark.Client:
    """创建飞书客户端"""
    if config is None:
        config = load_feishu_config()

    return lark.Client.builder() \
        .app_id(config["app_id"]) \
        .app_secret(config["app_secret"]) \
        .build()


def list_sheets(spreadsheet_token: str, config: dict = None) -> list[dict]:
    """
    获取电子表格中的所有 sheet 信息。

    Args:
        spreadsheet_token: 电子表格的 token
        config: 飞书配置，如果为 None 则自动加载

    Returns:
        Sheet 列表，每个 sheet 包含 sheet_id, title, index 等信息
    """
    client = get_client(config)

    # 使用 spreadsheet_sheet 的 query API
    from lark_oapi.api.sheets.v3 import QuerySpreadsheetSheetRequest

    request = QuerySpreadsheetSheetRequest.builder() \
        .spreadsheet_token(spreadsheet_token) \
        .build()

    # 发起请求
    response = client.sheets.v3.spreadsheet_sheet.query(request)

    if not response.success():
        log.error(f"获取表格信息失败: {response.code} - {response.msg}")
        raise Exception(f"API 调用失败: {response.msg}")

    # 提取 sheet 信息
    sheets = []
    if response.data and response.data.sheets:
        for sheet in response.data.sheets:
            sheets.append({
                "sheet_id": sheet.sheet_id,
                "title": sheet.title,
                "index": sheet.index if hasattr(sheet, 'index') else 0
            })

    return sheets


def print_sheet_info(spreadsheet_token: str):
    """
    打印电子表格中所有 sheet 的信息。

    Args:
        spreadsheet_token: 电子表格的 token
    """
    try:
        sheets = list_sheets(spreadsheet_token)

        print(f"\n电子表格包含 {len(sheets)} 个 sheet:\n")
        for i, sheet in enumerate(sheets, 1):
            print(f"{i}. {sheet['title']}")
            print(f"   sheet_id: {sheet['sheet_id']}")
            print(f"   index: {sheet['index']}")
            print()

        return sheets

    except Exception as e:
        log.error(f"获取 sheet 信息失败: {e}")
        raise


def get_access_token(config: dict = None) -> bool:
    """
    测试飞书连接（通过尝试获取一个表格信息）。

    Args:
        config: 飞书配置

    Returns:
        连接是否成功
    """
    if config is None:
        config = load_feishu_config()

    client = get_client(config)

    # 通过尝试获取表格信息来验证连接
    try:
        # 如果有 spreadsheet_token，尝试获取表格信息
        if config.get("spreadsheet_token"):
            request = GetSpreadsheetRequest.builder() \
                .spreadsheet_token(config["spreadsheet_token"]) \
                .build()
            response = client.sheets.v3.spreadsheet.get(request)
            if not response.success():
                raise Exception(f"API 调用失败: {response.msg}")
            return True
        else:
            # 如果没有 spreadsheet_token，只验证 client 创建成功
            return True
    except Exception as e:
        raise Exception(f"连接失败，请检查 app_id 和 app_secret: {e}")
