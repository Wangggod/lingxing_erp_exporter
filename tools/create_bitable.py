"""创建飞书多维表格用于存储每日汇总数据"""

import sys
from pathlib import Path

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.bitable_helper import create_summary_bitable
from scripts.logger import setup_logger

log = setup_logger()


def main():
    PRODUCT_NAME = "半开猫砂盆"

    log.info("=" * 60)
    log.info(f"开始创建多维表格: {PRODUCT_NAME}-每日汇总")
    log.info("=" * 60)

    try:
        result = create_summary_bitable(PRODUCT_NAME)

        print("\n" + "=" * 60)
        print("🎉 创建成功！")
        print("=" * 60)
        print(f"多维表格名称: {result['name']}")
        print(f"app_token: {result['app_token']}")
        print(f"table_id: {result['table_id']}")
        print(f"\n访问链接: {result['url']}")
        print("=" * 60)

        # 保存配置
        config_path = Path(__file__).resolve().parent.parent / "config" / "bitable.json"
        import json
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump({
                "product": PRODUCT_NAME,
                "app_token": result['app_token'],
                "table_id": result['table_id'],
                "url": result['url']
            }, f, indent=2, ensure_ascii=False)

        print(f"\n配置已保存到: {config_path}")

    except Exception as e:
        log.error(f"创建失败: {e}")
        raise


if __name__ == "__main__":
    main()
