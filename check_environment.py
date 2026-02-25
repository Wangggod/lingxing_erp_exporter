"""环境检查脚本 - 确保所有依赖都已正确安装"""

import sys
import subprocess
from pathlib import Path


def check_python_version():
    """检查 Python 版本"""
    print("🔍 检查 Python 版本...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 9:
        print(f"  ✅ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"  ❌ Python 版本过低: {version.major}.{version.minor}.{version.micro}")
        print(f"     需要 Python 3.9+")
        return False


def check_virtual_env():
    """检查是否在虚拟环境中"""
    print("\n🔍 检查虚拟环境...")
    in_venv = hasattr(sys, 'real_prefix') or (
        hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix
    )
    if in_venv:
        print(f"  ✅ 虚拟环境已激活: {sys.prefix}")
        return True
    else:
        print(f"  ⚠️  未在虚拟环境中")
        print(f"     建议运行: source venv/bin/activate")
        return True  # 警告但不阻止


def check_packages():
    """检查必要的 Python 包"""
    print("\n🔍 检查 Python 包...")
    required_packages = [
        "playwright",
        "pandas",
        "requests",
        "openpyxl",
    ]

    all_installed = True
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} 未安装")
            all_installed = False

    if not all_installed:
        print("\n  安装缺失的包:")
        print("  pip install -r requirements.txt")

    return all_installed


def check_playwright_browser():
    """检查 Playwright 浏览器"""
    print("\n🔍 检查 Playwright 浏览器...")
    try:
        result = subprocess.run(
            ["playwright", "install", "--dry-run", "chromium"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if "is already installed" in result.stdout or result.returncode == 0:
            print("  ✅ Chromium 浏览器已安装")
            return True
        else:
            print("  ❌ Chromium 浏览器未安装")
            print("     运行: playwright install chromium")
            return False
    except Exception as e:
        print(f"  ⚠️  无法检查浏览器状态: {e}")
        print("     运行: playwright install chromium")
        return False


def check_config_files():
    """检查配置文件"""
    print("\n🔍 检查配置文件...")
    config_dir = Path("config")
    required_configs = ["config.json", "feishu.json", "bitable.json"]

    all_exist = True
    for config_file in required_configs:
        config_path = config_dir / config_file
        if config_path.exists():
            print(f"  ✅ {config_file}")
            # 检查文件权限
            import stat
            mode = oct(config_path.stat().st_mode)[-3:]
            if mode != '600':
                print(f"     ⚠️  权限: {mode} (建议: 600)")
                print(f"     运行: chmod 600 {config_path}")
        else:
            print(f"  ❌ {config_file} 不存在")
            all_exist = False

    if not all_exist:
        print("\n  创建配置文件:")
        print("  cp config/config.example.json config/config.json")
        print("  # 然后编辑配置文件填入实际信息")

    return all_exist


def check_directories():
    """检查必要的目录"""
    print("\n🔍 检查目录结构...")
    required_dirs = ["data", "data/raw", "data/processed", "scripts", "config"]

    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        if dir_path.exists():
            print(f"  ✅ {dir_name}/")
        else:
            print(f"  ⚠️  {dir_name}/ 不存在，将自动创建")
            dir_path.mkdir(parents=True, exist_ok=True)

    return True


def check_disk_space():
    """检查磁盘空间"""
    print("\n🔍 检查磁盘空间...")
    try:
        import shutil
        total, used, free = shutil.disk_usage("/")
        free_gb = free // (2**30)

        if free_gb >= 10:
            print(f"  ✅ 可用空间: {free_gb} GB")
            return True
        else:
            print(f"  ⚠️  可用空间不足: {free_gb} GB")
            print(f"     建议至少 10 GB")
            return False
    except Exception as e:
        print(f"  ⚠️  无法检查磁盘空间: {e}")
        return True


def main():
    print("=" * 60)
    print("🔧 环境检查 - 领星数据自动化系统")
    print("=" * 60)

    checks = [
        ("Python 版本", check_python_version),
        ("虚拟环境", check_virtual_env),
        ("Python 包", check_packages),
        ("Playwright 浏览器", check_playwright_browser),
        ("配置文件", check_config_files),
        ("目录结构", check_directories),
        ("磁盘空间", check_disk_space),
    ]

    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n❌ {name} 检查失败: {e}")
            results.append((name, False))

    # 总结
    print("\n" + "=" * 60)
    print("📊 检查结果总结")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅" if result else "❌"
        print(f"{status} {name}")

    print("=" * 60)
    print(f"通过: {passed}/{total}")

    if passed == total:
        print("\n🎉 环境检查全部通过！可以开始运行了。")
        return 0
    else:
        print("\n⚠️  部分检查未通过，请根据上述提示修复问题。")
        return 1


if __name__ == "__main__":
    sys.exit(main())
