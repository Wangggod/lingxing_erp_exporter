# Playwright Exporter

使用 Playwright 自动登录网站并导出文件的最小工具。

## 项目结构

```
playwright-scraper/
├── config/
│   ├── config.example.json   # 配置模板
│   └── config.json           # 实际配置（需自行创建，已 gitignore）
├── scripts/
│   ├── exporter.py           # 核心逻辑
│   └── logger.py             # 日志配置
├── data/raw/                 # 下载文件存放目录
├── logs/                     # 运行日志
├── state.json                # 浏览器登录状态（自动生成）
├── main.py                   # 入口
└── requirements.txt
```

## 安装

```bash
# 创建虚拟环境（推荐）
python3.11 -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 安装浏览器（首次需要）
playwright install chromium
```

## 配置

```bash
cp config/config.example.json config/config.json
```

编辑 `config/config.json`，填写目标网站的 URL、账号密码和页面元素选择器：

| 字段 | 说明 |
|------|------|
| `url` | 登录页地址 |
| `username` / `password` | 账号密码 |
| `selectors.username_input` | 用户名输入框 CSS 选择器 |
| `selectors.password_input` | 密码输入框 CSS 选择器 |
| `selectors.login_button` | 登录按钮 CSS 选择器 |
| `selectors.export_button` | 导出按钮 CSS 选择器 |
| `download_timeout_ms` | 下载超时（毫秒），默认 60000 |
| `headless` | 是否无头模式，默认 false |

## 运行

```bash
python main.py
```

首次运行会执行登录流程，成功后自动保存 `state.json`。后续运行会复用登录状态，跳过登录步骤。

如果登录状态过期，删除 `state.json` 重新运行即可。

## 日志

运行日志保存在 `logs/export.log`，同时输出到终端。
