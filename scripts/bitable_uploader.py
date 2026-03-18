"""上传数据到飞书多维表格 - 支持幂等性和重试"""

import time
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from scripts.bitable_helper import get_tenant_access_token
from scripts.feishu_helper import load_feishu_config
from scripts.logger import setup_logger
import requests
import json

log = setup_logger()


class BitableAPIError(Exception):
    """飞书 API 错误"""
    pass


class RateLimitError(Exception):
    """速率限制错误"""
    pass


def generate_unique_key(date_str: str, country: str, product_name: str = None) -> str:
    """
    生成唯一键。

    Args:
        date_str: 日期字符串（YYYY-MM-DD）
        country: 国家
        product_name: 品名（可选，有则加入 key）

    Returns:
        唯一键字符串（格式：YYYY-MM-DD|国家|品名 或 YYYY-MM-DD|国家）
    """
    if product_name:
        return f"{date_str}|{country}|{product_name}"
    return f"{date_str}|{country}"


def query_existing_records(
    app_token: str,
    table_id: str,
    access_token: str,
    date_str: str,
    country: str
) -> Optional[Dict]:
    """
    查询指定日期和国家的现有记录（使用 unique_key 精确查询）。

    Args:
        app_token: 多维表格 token
        table_id: 数据表 ID
        access_token: 访问令牌
        date_str: 日期字符串（YYYY-MM-DD）
        country: 国家

    Returns:
        如果存在返回记录字典，否则返回 None
    """
    unique_key = generate_unique_key(date_str, country)

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # 使用 filter 精确查询 unique_key（O(1) 复杂度）
    payload = {
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": "unique_key",
                    "operator": "is",
                    "value": [unique_key]
                }
            ]
        },
        "page_size": 1  # 只需要查询 1 条
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        result = response.json()

        if result.get("code") == 429:
            raise RateLimitError("速率限制")

        if result.get("code") != 0:
            log.warning(f"查询记录失败: {result}")
            return None

        # 获取匹配的记录
        items = result.get("data", {}).get("items", [])
        if items:
            item = items[0]
            log.info(f"✓ 找到现有记录: {unique_key}, record_id={item['record_id']}")
            return item

        log.debug(f"✗ 未找到记录: {unique_key}")
        return None

    except RateLimitError:
        raise
    except Exception as e:
        log.warning(f"查询记录时出错: {e}")
        return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((RateLimitError, requests.exceptions.RequestException)),
    reraise=True
)
def create_record_with_retry(
    app_token: str,
    table_id: str,
    access_token: str,
    fields: Dict
) -> Dict:
    """
    创建记录（带重试）。

    Args:
        app_token: 多维表格 token
        table_id: 数据表 ID
        access_token: 访问令牌
        fields: 字段数据

    Returns:
        创建结果
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {"fields": fields}

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    result = response.json()

    if result.get("code") == 429:
        log.warning("遇到速率限制，等待重试...")
        raise RateLimitError("速率限制")

    if result.get("code") != 0:
        raise BitableAPIError(f"创建记录失败: {result}")

    return result


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((RateLimitError, requests.exceptions.RequestException)),
    reraise=True
)
def update_record_with_retry(
    app_token: str,
    table_id: str,
    record_id: str,
    access_token: str,
    fields: Dict
) -> Dict:
    """
    更新记录（带重试）。

    Args:
        app_token: 多维表格 token
        table_id: 数据表 ID
        record_id: 记录 ID
        access_token: 访问令牌
        fields: 字段数据

    Returns:
        更新结果
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {"fields": fields}

    response = requests.put(url, headers=headers, json=payload, timeout=30)
    result = response.json()

    if result.get("code") == 429:
        log.warning("遇到速率限制，等待重试...")
        raise RateLimitError("速率限制")

    if result.get("code") != 0:
        raise BitableAPIError(f"更新记录失败: {result}")

    return result


def prepare_fields(row: pd.Series, field_mapping: dict = None) -> Dict:
    """
    准备字段数据（包含 unique_key）。

    Args:
        row: DataFrame 行
        field_mapping: 字段映射

    Returns:
        字段字典
    """
    if field_mapping is None:
        field_mapping = {}

    fields = {}
    date_str = None
    country_str = None
    product_name_str = None

    for col_name, value in row.items():
        # 应用字段映射
        field_name = field_mapping.get(col_name, col_name)

        # 处理不同类型的值
        if pd.isna(value):
            # 跳过空值
            continue
        elif col_name == '站点日期':
            # 日期字段：转换为时间戳（毫秒）
            import datetime
            date_obj = pd.to_datetime(value)
            timestamp_ms = int(date_obj.timestamp() * 1000)
            fields[field_name] = timestamp_ms
            # 保存日期字符串用于生成 unique_key
            date_str = value if isinstance(value, str) else date_obj.strftime('%Y-%m-%d')
        elif col_name in ['国家', '货币', '品名']:
            # 文本字段
            fields[field_name] = str(value)
            if col_name == '国家':
                country_str = str(value)
            elif col_name == '品名':
                product_name_str = str(value)
        else:
            # 数字字段
            if isinstance(value, (int, float)):
                fields[field_name] = value
            else:
                # 尝试转换为数字
                try:
                    fields[field_name] = float(value)
                except:
                    fields[field_name] = str(value)

    # 添加 unique_key
    if date_str and country_str:
        fields['unique_key'] = generate_unique_key(date_str, country_str, product_name_str)

    return fields


def upsert_to_bitable(
    csv_path: Path,
    app_token: str,
    table_id: str,
    access_token: str,
    field_mapping: dict = None
) -> dict:
    """
    上传 CSV 数据到飞书多维表格（支持幂等性）。

    对于每一行：
    1. 查询是否存在（基于日期+国家）
    2. 存在则更新，不存在则创建

    Args:
        csv_path: CSV 文件路径
        app_token: 多维表格 token
        table_id: 数据表 ID
        access_token: 访问令牌
        field_mapping: CSV 列名到多维表格字段名的映射（可选）

    Returns:
        上传结果统计
    """
    # 读取 CSV
    df = pd.read_csv(csv_path)
    log.info(f"读取 CSV: {len(df)} 行数据")

    total_created = 0
    total_updated = 0
    total_failed = 0
    failed_records = []

    for idx, row in df.iterrows():
        date_str = str(row['站点日期'])
        country = str(row['国家'])

        try:
            # 准备字段数据
            fields = prepare_fields(row, field_mapping)

            # 查询是否存在
            existing = query_existing_records(
                app_token, table_id, access_token,
                date_str, country
            )

            if existing:
                # 更新现有记录
                log.info(f"更新记录: {date_str} - {country}")
                update_record_with_retry(
                    app_token, table_id, existing['record_id'],
                    access_token, fields
                )
                total_updated += 1
            else:
                # 创建新记录
                log.info(f"创建记录: {date_str} - {country}")
                create_record_with_retry(
                    app_token, table_id, access_token, fields
                )
                total_created += 1

            # 避免触发速率限制
            time.sleep(0.2)

        except Exception as e:
            log.error(f"❌ 处理失败 [{date_str} - {country}]: {e}")
            total_failed += 1
            failed_records.append({
                "date": date_str,
                "country": country,
                "error": str(e)
            })

    log.info("=" * 60)
    log.info(f"上传完成")
    log.info(f"  创建: {total_created} 条")
    log.info(f"  更新: {total_updated} 条")
    log.info(f"  失败: {total_failed} 条")
    log.info("=" * 60)

    if failed_records:
        log.warning(f"失败的记录: {failed_records}")

    return {
        "total": len(df),
        "created": total_created,
        "updated": total_updated,
        "failed": total_failed,
        "failed_records": failed_records
    }


def mark_upload_success(csv_path: Path):
    """
    标记上传成功。

    创建一个 .success 文件，记录上传时间和文件信息。
    """
    success_file = csv_path.parent / f"{csv_path.stem}.success"
    import datetime
    with open(success_file, 'w', encoding='utf-8') as f:
        json.dump({
            "uploaded_at": datetime.datetime.now().isoformat(),
            "csv_file": str(csv_path),
            "success": True
        }, f, indent=2)
    log.info(f"✅ 标记上传成功: {success_file}")


def check_already_uploaded(csv_path: Path) -> bool:
    """
    检查是否已经上传过。

    Returns:
        True 如果已上传，False 如果未上传
    """
    success_file = csv_path.parent / f"{csv_path.stem}.success"
    if success_file.exists():
        try:
            with open(success_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                log.info(f"⚠️  检测到已上传标记: {data.get('uploaded_at')}")
                return True
        except:
            return False
    return False


def upload_summary_to_bitable(
    csv_path: Path,
    bitable_config: dict = None,
    force: bool = False
) -> dict:
    """
    上传每日汇总数据到多维表格（幂等性保证）。

    Args:
        csv_path: daily_summary.csv 路径
        bitable_config: 多维表格配置（包含 app_token 和 table_id）
        force: 是否强制上传（忽略已上传标记）

    Returns:
        上传结果
    """
    # 检查是否已上传
    if not force and check_already_uploaded(csv_path):
        log.warning("数据已上传，跳过。使用 force=True 强制重新上传。")
        return {
            "skipped": True,
            "reason": "already_uploaded"
        }

    if bitable_config is None:
        # 从配置文件读取
        config_path = Path(__file__).parent.parent / "config" / "bitable.json"
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                bitable_config = json.load(f)
        else:
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

    # 获取飞书配置
    feishu_config = load_feishu_config()
    access_token = get_tenant_access_token(
        feishu_config["app_id"],
        feishu_config["app_secret"]
    )

    # 上传数据（upsert）
    result = upsert_to_bitable(
        csv_path,
        bitable_config["app_token"],
        bitable_config["table_id"],
        access_token
    )

    # 如果全部成功，标记上传成功
    if result["failed"] == 0:
        mark_upload_success(csv_path)

    return result
