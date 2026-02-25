import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright, BrowserContext, Page

from scripts.logger import setup_logger

ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT / "state.json"
DOWNLOAD_DIR = ROOT / "data" / "raw"

log = setup_logger()


def get_target_date() -> str:
    """取美西时间当前日期的前一天，返回 YYYY-MM-DD。"""
    now_shanghai = datetime.now(ZoneInfo("Asia/Shanghai"))
    now_la = now_shanghai.astimezone(ZoneInfo("America/Los_Angeles"))
    return (now_la - timedelta(days=1)).strftime("%Y-%m-%d")


def load_config() -> dict:
    cfg_path = ROOT / "config" / "config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"配置文件不存在: {cfg_path}\n请复制 config.example.json 为 config.json 并填写实际值"
        )
    return json.loads(cfg_path.read_text(encoding="utf-8"))


# ── 登录 ──────────────────────────────────────────────

def _create_context(playwright, cfg: dict) -> BrowserContext:
    browser = playwright.chromium.launch(headless=cfg.get("headless", False))
    if STATE_FILE.exists():
        log.info("检测到 state.json，尝试复用登录状态")
        return browser.new_context(storage_state=str(STATE_FILE))
    return browser.new_context()


def _get_cookies(ctx: BrowserContext) -> dict[str, str]:
    return {c["name"]: c["value"] for c in ctx.cookies()}


def _is_logged_in(ctx: BrowserContext) -> bool:
    cookies = _get_cookies(ctx)
    return bool(cookies.get("auth-token"))


def _login(page: Page, cfg: dict) -> None:
    sel = cfg["selectors"]
    page.fill(sel["username_input"], cfg["username"])
    page.fill(sel["password_input"], cfg["password"])
    page.click(sel["login_button"])
    page.wait_for_load_state("networkidle")


def _ensure_logged_in(page: Page, ctx: BrowserContext, cfg: dict) -> None:
    page.goto(cfg["url"], wait_until="domcontentloaded")

    if _is_logged_in(ctx):
        log.info("已处于登录状态")
        return

    if STATE_FILE.exists():
        log.info("登录状态已过期，重新登录")
        STATE_FILE.unlink()

    for attempt in range(2):
        log.info("执行登录（第 %d 次）", attempt + 1)
        _login(page, cfg)

        if _is_logged_in(ctx):
            log.info("登录成功")
            ctx.storage_state(path=str(STATE_FILE))
            log.info("登录状态已保存到 state.json")
            return

        if attempt == 0:
            log.warning("登录后未检测到 auth-token，重试一次")
            page.goto(cfg["url"], wait_until="domcontentloaded")

    raise RuntimeError("登录失败，已达到最大重试次数")


# ── 页面稳定 ──────────────────────────────────────────

def _navigate_to_app(page: Page, cfg: dict) -> None:
    app_url = cfg.get("app_url", cfg["url"].replace("/login", "/erp"))
    page.goto(app_url, wait_until="domcontentloaded")
    _ensure_page_stable(page)


def _ensure_page_stable(page: Page) -> None:
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_function("typeof window.axios !== 'undefined'", timeout=30000)
    page.wait_for_timeout(1500)
    log.info("页面已稳定")


# ── XHR 通用 ─────────────────────────────────────────

_XHR_POST_JS = """([url, body]) => {
    function getCookie(name) {
        const m = document.cookie.match(new RegExp('(?:^|; )' + name + '=([^;]*)'));
        return m ? decodeURIComponent(m[1]) : '';
    }
    return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', url, true);
        xhr.setRequestHeader('Content-Type', 'application/json');
        xhr.setRequestHeader('auth-token', getCookie('auth-token'));
        xhr.setRequestHeader('X-AK-Company-Id', getCookie('company_id'));
        xhr.setRequestHeader('X-AK-Env-Key', getCookie('envKey'));
        xhr.setRequestHeader('authToken', getCookie('authToken'));
        xhr.setRequestHeader('token', getCookie('token'));
        xhr.withCredentials = true;
        xhr.onload = () => resolve(JSON.parse(xhr.responseText));
        xhr.onerror = () => reject(new Error('XHR failed: ' + xhr.status));
        xhr.send(JSON.stringify(body));
    });
}"""


def _xhr_post_with_retry(page: Page, url: str, body: dict) -> dict:
    """执行 XHR POST，遇到页面导航重试一次。"""
    for attempt in range(2):
        try:
            return page.evaluate(_XHR_POST_JS, [url, body])
        except Exception as e:
            err_msg = str(e)
            if "Execution context was destroyed" in err_msg or "most likely because of a navigation" in err_msg:
                if attempt == 0:
                    log.warning("页面导航导致上下文丢失，等待页面稳定后重试...")
                    _ensure_page_stable(page)
                    continue
            raise


# ── 订单利润（API 直接返回 report_id）────────────────

def export_order_profit(page: Page, report: dict, target_date: str) -> str:
    body = {
        "start_date": target_date,
        "end_date": target_date,
        **report.get("extra_params", {}),
    }

    result = _xhr_post_with_retry(page, report["export_url"], body)

    if result.get("code") != 1:
        raise RuntimeError(f"[{report['name']}] 导出任务创建失败: {result}")

    report_id = result["data"]["report_id"]
    log.info("[%s] 导出任务已创建, report_id=%s", report["name"], report_id)
    return report_id


# ── 订单列表（从通知弹窗 DOM 提取 report_id）────────

_NOTIFICATION_SELECTOR = ".el-notification__content span[data-id]"


def export_order_list(page: Page, report: dict, target_date: str) -> str:
    # 先清除页面上已有的通知弹窗
    page.evaluate("""() => {
        document.querySelectorAll('.el-notification').forEach(n => n.remove());
    }""")

    body = {
        "startDate": target_date,
        "endDate": target_date,
        **report.get("extra_params", {}),
    }

    result = _xhr_post_with_retry(page, report["export_url"], body)

    if result.get("code") != 1:
        raise RuntimeError(f"[{report['name']}] 导出任务创建失败: {result}")

    log.info("[%s] 导出请求已发送，等待通知弹窗...", report["name"])

    # 主路径：等待通知弹窗中的 span[data-id] 出现
    try:
        el = page.wait_for_selector(_NOTIFICATION_SELECTOR, timeout=30000)
        report_id = el.get_attribute("data-id")
        if report_id:
            log.info("[%s] 从通知弹窗获取 report_id=%s", report["name"], report_id)
            return report_id
    except Exception:
        log.warning("[%s] 通知弹窗未出现，尝试兜底方案...", report["name"])

    # 兜底：点击"立即下载"，从弹出的下载 URL 中提取 report_id
    return _fallback_click_download(page, report["name"])


def _fallback_click_download(page: Page, name: str) -> str:
    """兜底：点击通知中的立即下载链接，从新窗口 URL 提取 report_id。"""
    try:
        link = page.wait_for_selector(
            ".el-notification__content span.ak-blue-pointer[data-id]",
            timeout=10000,
        )
        report_id = link.get_attribute("data-id")
        if report_id:
            log.info("[%s] 兜底：从链接 data-id 获取 report_id=%s", name, report_id)
            return report_id
    except Exception:
        pass

    raise RuntimeError(f"[{name}] 无法获取 report_id：通知弹窗和兜底均失败")


# ── 下载 ─────────────────────────────────────────────

def download_report(page: Page, cfg: dict, report_id: str, report_name: str, report_type: str, target_date: str) -> Path:
    download_url = cfg["download_url"]
    wait = cfg.get("download_wait_seconds", 10)

    log.info("[%s] 等待 %d 秒后下载...", report_name, wait)
    time.sleep(wait)

    url = f"{download_url}?report_id={report_id}"

    # 创建按日期的子目录
    date_dir = DOWNLOAD_DIR / target_date
    date_dir.mkdir(parents=True, exist_ok=True)

    with page.expect_download(timeout=60000) as dl_info:
        page.evaluate("url => window.open(url)", url)
    download = dl_info.value

    # 使用固定的文件名：order_profit.xlsx 或 order_list.xlsx
    filename = f"{report_type}.xlsx"
    dest = date_dir / filename
    download.save_as(str(dest))
    log.info("[%s] 文件已保存: %s", report_name, dest)
    return dest


# ── 主流程 ────────────────────────────────────────────

# 报表类型 → 导出函数映射
_EXPORT_HANDLERS = {
    "order_profit": export_order_profit,
    "order_list": export_order_list,
}


def run_for_date(target_date: str) -> None:
    """指定日期执行导出任务"""
    cfg = load_config()
    log.info("开始执行导出任务")

    with sync_playwright() as pw:
        ctx = _create_context(pw, cfg)
        page = ctx.new_page()

        try:
            _ensure_logged_in(page, ctx, cfg)
            _navigate_to_app(page, cfg)

            log.info("目标日期: %s", target_date)

            for report in cfg["reports"]:
                name = report["name"]
                rtype = report["type"]
                handler = _EXPORT_HANDLERS.get(rtype)
                if not handler:
                    log.error("[%s] 未知报表类型: %s，跳过", name, rtype)
                    continue

                log.info("[%s] 开始导出...", name)
                report_id = handler(page, report, target_date)
                download_report(page, cfg, report_id, name, rtype, target_date)

        except Exception:
            log.exception("任务执行失败")
            raise
        finally:
            ctx.close()

    log.info("全部任务完成")


def run() -> None:
    """使用默认日期（昨天）执行导出任务"""
    target_date = get_target_date()
    run_for_date(target_date)
