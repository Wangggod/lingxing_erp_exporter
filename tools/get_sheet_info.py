"""获取飞书表格的 sheet 信息

使用方法：
1. 在 config/feishu.json 中填写 spreadsheet_token
2. 运行此脚本：python tools/get_sheet_info.py
3. 脚本会列出所有 sheet 的信息，然后更新配置文件
"""

import sys
from pathlib import Path

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
from scripts.feishu_helper import print_sheet_info, get_access_token
from scripts.logger import setup_logger

log = setup_logger()


def main():
    config_path = Path(__file__).resolve().parent.parent / "config" / "feishu.json"

    # 加载配置
    if not config_path.exists():
        log.error(f"配置文件不存在: {config_path}")
        print("\n请先创建 config/feishu.json 配置文件")
        return

    config = json.loads(config_path.read_text(encoding="utf-8"))

    # 检查 spreadsheet_token
    if not config.get("spreadsheet_token"):
        print("\n❌ 请先在 config/feishu.json 中填写 spreadsheet_token")
        print("\n获取方式：")
        print("1. 打开飞书表格")
        print("2. 从 URL 中获取 token:")
        print("   https://xxx.feishu.cn/sheets/{spreadsheet_token}")
        return

    print("正在连接飞书 API...")

    # 测试连接
    try:
        token = get_access_token(config)
        print("✅ 连接成功！\n")
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        print("\n请检查 config/feishu.json 中的 app_id 和 app_secret")
        return

    # 获取 sheet 信息
    try:
        sheets = print_sheet_info(config["spreadsheet_token"])

        # 自动匹配并更新配置
        print("正在匹配 sheet...")

        for sheet in sheets:
            title = sheet["title"]
            sheet_id = sheet["sheet_id"]

            # 根据标题匹配
            if "订单列表" in title or "order_list" in title.lower():
                config["sheets"]["order_list"]["sheet_id"] = sheet_id
                config["sheets"]["order_list"]["name"] = title
                print(f"✅ 匹配到订单列表: {title}")

            elif "订单利润" in title or "order_profit" in title.lower() or "利润" in title:
                config["sheets"]["order_profit"]["sheet_id"] = sheet_id
                config["sheets"]["order_profit"]["name"] = title
                print(f"✅ 匹配到订单利润: {title}")

        # 保存配置
        config_path.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\n✅ 配置已更新: {config_path}")

        # 显示最终配置
        print("\n当前配置:")
        print(f"订单列表 sheet_id: {config['sheets']['order_list']['sheet_id']}")
        print(f"订单利润 sheet_id: {config['sheets']['order_profit']['sheet_id']}")

    except Exception as e:
        print(f"❌ 获取 sheet 信息失败: {e}")


if __name__ == "__main__":
    main()
