import os
import shutil
from urllib.parse import parse_qs

from aliyun_token_ingest_daily import ingest_yesterday


def format_cookie_header(cookies: list[dict]) -> str:
    parts = []
    for c in cookies:
        name = c.get("name")
        value = c.get("value")
        if name and value is not None:
            parts.append(f"{name}={value}")
    return "; ".join(parts)


def pick_csrf_token(headers: dict, cookies: list[dict]) -> str | None:
    header_token = headers.get("x-csrf-token") or headers.get("x-csrf-token".lower())
    if header_token:
        return header_token
    cookie_map = {c.get("name"): c.get("value") for c in cookies}
    return cookie_map.get("c_csrf_token") or cookie_map.get("login_aliyunid_csrf")

def ensure_user_data_dir(user_data_dir: str, source_dir: str | None, profile_dir: str):
    if os.path.exists(user_data_dir) and os.listdir(user_data_dir):
        return
    if not source_dir:
        raise RuntimeError(
            "CHROME_USER_DATA_DIR must be a non-default path. "
            "Set CHROME_USER_DATA_DIR_SOURCE to your default Chrome User Data to copy login state."
        )
    os.makedirs(user_data_dir, exist_ok=True)
    src_profile = os.path.join(source_dir, profile_dir)
    dst_profile = os.path.join(user_data_dir, profile_dir)
    if not os.path.exists(src_profile):
        raise RuntimeError(f"Source profile not found: {src_profile}")
    local_state_src = os.path.join(source_dir, "Local State")
    local_state_dst = os.path.join(user_data_dir, "Local State")
    if os.path.exists(local_state_src) and not os.path.exists(local_state_dst):
        shutil.copy2(local_state_src, local_state_dst)
    if not os.path.exists(dst_profile):
        shutil.copytree(src_profile, dst_profile)


def main():
    try:
        from playwright.sync_api import sync_playwright  # pylint: disable=import-error
    except ImportError as exc:
        raise RuntimeError(
            "Playwright not installed. Run: pip install playwright && python -m playwright install chromium"
        ) from exc

    target_url = (
        "https://bailian.console.aliyun.com/cn-beijing/"
        "?spm=5176.28197619.console-base_search-panel.dtab-product_sfm.73993ae4MhpTct"
        "&scm=20140722.S_sfm._.ID_sfm-RL_%E7%99%BE%E7%82%BC-LOC_console_console-OR_ser-V_4-P0_0"
        "&tab=model#/model-usage/usage-statistics"
    )
    api_keyword = "bailian-telemetry.model.getModelUsageStatistic"

    user_data_dir = os.getenv("CHROME_USER_DATA_DIR")
    profile_dir = os.getenv("CHROME_PROFILE_DIR", "Default")
    source_user_data_dir = os.getenv("CHROME_USER_DATA_DIR_SOURCE")
    chrome_exe = os.getenv("CHROME_EXECUTABLE_PATH")

    if not user_data_dir:
        raise RuntimeError("Missing CHROME_USER_DATA_DIR (Chrome User Data path).")
    ensure_user_data_dir(user_data_dir, source_user_data_dir, profile_dir)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir,
            channel=None if chrome_exe else "chrome",
            executable_path=chrome_exe,
            headless=False,
            args=[
                f"--profile-directory={profile_dir}",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-session-crashed-bubble",
                "--disable-features=ChromeWhatsNewUI",
            ],
        )
        def open_target_page():
            page = context.pages[-1] if context.pages else context.new_page()
            print("打开目标页面:", target_url)
            try:
                page.goto(target_url, wait_until="domcontentloaded", timeout=60 * 1000)
            except Exception as exc:
                print("goto failed:", exc)
                try:
                    page.evaluate("url => { window.location.href = url; }", target_url)
                except Exception as exc2:
                    print("evaluate failed:", exc2)
            page.wait_for_timeout(3000)
            print("当前页面:", page.url)
            return page

        page = open_target_page()
        if page.url.startswith("about:"):
            print("仍在 about 页面，尝试再次跳转...")
            page.evaluate("url => { window.location.href = url; }", target_url)
            page.wait_for_timeout(3000)
            print("当前页面:", page.url)

        if "aliyun.com" in page.url and "login" in page.url:
            print("检测到登录页，请先完成登录。")
            input("登录完成后按回车继续...")
            page = context.pages[-1] if context.pages else page
            page.goto(target_url, wait_until="domcontentloaded", timeout=60 * 1000)
            print("当前页面:", page.url)

        print("请在打开的浏览器中完成登录，并进入“模型用量统计”页面。")
        print("脚本会自动捕获请求，然后直接入库。")

        if hasattr(page, "wait_for_request"):
            request = page.wait_for_request(
                lambda r: api_keyword in r.url and r.method.lower() == "post",
                timeout=10 * 60 * 1000,
            )
        else:
            request = page.wait_for_event(
                "request",
                predicate=lambda r: api_keyword in r.url and r.method.lower() == "post",
                timeout=10 * 60 * 1000,
            )

        post_data = request.post_data or ""
        parsed = parse_qs(post_data)
        region = parsed.get("region", [""])[0]
        sec_token = parsed.get("sec_token", [""])[0]

        cookies = context.cookies()
        cookie_header = format_cookie_header(cookies)
        csrf_token = pick_csrf_token(request.headers, cookies)

        print("捕获成功，开始入库（昨日）。")
        ingest_yesterday(
            os.environ["ALIYUN_BAILIAN_WORKSPACE_ID"],
            url=request.url,
            cookie=cookie_header,
            sec_token=sec_token or None,
            csrf_token=csrf_token or None,
            region=region or None,
        )

        page.wait_for_timeout(2000)
        context.close()


if __name__ == "__main__":
    main()
