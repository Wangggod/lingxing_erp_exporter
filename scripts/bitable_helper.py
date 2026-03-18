"""飞书多维表格 API 工具"""

import json
import requests
from pathlib import Path
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


def create_bitable(name: str, folder_token: str = None, access_token: str = None) -> dict:
    """
    创建多维表格。

    Args:
        name: 多维表格名称
        folder_token: 文件夹 token（可选）
        access_token: 访问令牌

    Returns:
        创建结果，包含 app_token
    """
    url = "https://open.feishu.cn/open-apis/bitable/v1/apps"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "name": name
    }

    if folder_token:
        payload["folder_token"] = folder_token

    response = requests.post(url, headers=headers, json=payload)
    result = response.json()

    if result.get("code") != 0:
        log.error(f"创建多维表格失败: {result}")
        raise Exception(f"创建多维表格失败: {result.get('msg', 'Unknown error')}")

    log.info(f"✅ 创建多维表格成功: {name}")
    log.info(f"   app_token: {result['data']['app']['app_token']}")
    log.info(f"   url: {result['data']['app']['url']}")

    return result["data"]["app"]


def create_table(app_token: str, table_name: str, access_token: str) -> dict:
    """
    在多维表格中创建数据表。

    Args:
        app_token: 多维表格 token
        table_name: 数据表名称
        access_token: 访问令牌

    Returns:
        创建结果，包含 table_id
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "table": {
            "name": table_name
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    result = response.json()

    if result.get("code") != 0:
        log.error(f"创建数据表失败: {result}")
        raise Exception(f"创建数据表失败: {result.get('msg', 'Unknown error')}")

    log.info(f"✅ 创建数据表成功: {table_name}")
    log.info(f"   table_id: {result['data']['table_id']}")

    return result["data"]


def add_field(app_token: str, table_id: str, field_name: str, field_type: int, access_token: str) -> dict:
    """
    添加字段到数据表。

    Args:
        app_token: 多维表格 token
        table_id: 数据表 ID
        field_name: 字段名称
        field_type: 字段类型
            1: 文本
            2: 数字
            5: 日期
        access_token: 访问令牌

    Returns:
        创建结果
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "field_name": field_name,
        "type": field_type
    }

    response = requests.post(url, headers=headers, json=payload)
    result = response.json()

    if result.get("code") != 0:
        log.error(f"添加字段失败 [{field_name}]: {result}")
        raise Exception(f"添加字段失败: {result.get('msg', 'Unknown error')}")

    log.info(f"✅ 添加字段: {field_name}")

    return result["data"]


def set_bitable_permission(app_token: str, access_token: str):
    """设置多维表格权限：组织内链接可阅读，并授予管理员完全访问权限。"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # 1. 组织内链接可阅读
    url = f"https://open.feishu.cn/open-apis/drive/v1/permissions/{app_token}/public"
    payload = {
        "external_access_entity": "open",
        "security_entity": "anyone_can_view",
        "comment_entity": "anyone_can_view",
        "share_entity": "anyone",
        "link_share_entity": "tenant_readable",
        "invite_external": True
    }
    response = requests.patch(url, headers=headers, json=payload, params={"type": "bitable"})
    if response.status_code == 200 and response.json().get("code") == 0:
        log.info("✅ 权限设置完成：组织内链接可阅读")
    else:
        log.warning(f"⚠️ 链接权限设置失败: {response.text}")

    # 2. 授予管理员完全访问权限
    admin_user_id = "ou_49b9c50170839a6ef4c87eaa015a7b5d"
    member_url = f"https://open.feishu.cn/open-apis/drive/v1/permissions/{app_token}/members"
    resp = requests.post(
        member_url,
        headers=headers,
        params={"type": "bitable", "need_notification": "false"},
        json={
            "member_type": "openid",
            "member_id": admin_user_id,
            "perm": "full_access"
        }
    )
    if resp.status_code == 200 and resp.json().get("code") == 0:
        log.info("✅ 管理员权限授予完成")
    else:
        log.warning(f"⚠️ 管理员权限授予失败: {resp.text}")


def create_summary_bitable(group_name: str, config: dict = None) -> dict:
    """
    创建每日汇总多维表格，包含所有字段。

    Args:
        group_name: 项目组名称
        config: 飞书配置

    Returns:
        包含 app_token 和 table_id 的字典
    """
    if config is None:
        from scripts.feishu_helper import load_feishu_config
        config = load_feishu_config()

    # 获取访问令牌
    access_token = get_tenant_access_token(config["app_id"], config["app_secret"])

    # 1. 创建多维表格
    bitable_name = f"{group_name}-每日汇总"
    app_info = create_bitable(bitable_name, access_token=access_token)
    app_token = app_info["app_token"]

    # 2. 获取默认数据表（创建多维表格时会自动创建一个默认表）
    # 需要重命名或删除默认表，然后创建我们的表
    # 为了简化，我们直接在默认表上添加字段

    # 先列出所有表
    list_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(list_url, headers=headers)
    result = response.json()

    if result.get("code") != 0:
        raise Exception(f"获取表列表失败: {result}")

    # 获取默认表的 table_id
    table_id = result["data"]["items"][0]["table_id"]
    log.info(f"使用默认数据表: {table_id}")

    # 3. 添加字段（跳过第一个默认的文本字段）
    fields = [
        ("unique_key", 1),      # 文本（幂等 upsert 用）
        ("站点日期", 5),        # 日期
        ("国家", 1),            # 文本
        ("品名", 1),            # 文本
        ("货币", 1),            # 文本
        ("总销量", 2),          # 数字
        ("FBM订单", 2),         # 数字
        ("FBA订单", 2),         # 数字
        ("广告单", 2),          # 数字
        ("总销售额", 2),        # 数字
        ("优惠券订单数", 2),    # 数字
        ("优惠券折扣总额", 2),  # 数字
        ("实际销售额", 2),      # 数字
        ("总平台佣金", 2),      # 数字
        ("总FBA费", 2),         # 数字
        ("总广告花费", 2),      # 数字
        ("今日退款数量", 2),    # 数字
        ("今日退款金额", 2),    # 数字
        ("FBM运费", 2),         # 数字
        ("总采购成本", 2),      # 数字
        ("总头程成本", 2),      # 数字
        ("回款", 2),            # 数字
        ("利润", 2),            # 数字
        ("TAcos", 2),           # 数字
        ("Sessions", 2),        # 数字
        ("PV", 2),              # 数字
        ("CPC", 2),             # 数字
        ("广告CVR", 2),         # 数字
    ]

    for field_name, field_type in fields:
        add_field(app_token, table_id, field_name, field_type, access_token)

    # 4. 设置权限：组织内链接可阅读
    set_bitable_permission(app_token, access_token)

    log.info("=" * 60)
    log.info("✅ 多维表格创建完成！")
    log.info(f"   名称: {bitable_name}")
    log.info(f"   app_token: {app_token}")
    log.info(f"   table_id: {table_id}")
    log.info(f"   URL: {app_info['url']}")
    log.info("=" * 60)

    return {
        "app_token": app_token,
        "table_id": table_id,
        "url": app_info["url"],
        "name": bitable_name
    }
