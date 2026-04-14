"""
Panel Auto-Login
Uses Chromium + Xvfb virtual display to login to the SMS panel and extract
session credentials (PHPSESSID + sesskey) without requiring a physical screen.
"""
import re
import time
import logging
import threading
from urllib.parse import urlparse

logger = logging.getLogger('PanelLogin')

# Only one Chromium browser may run at a time — prevents resource exhaustion
# when multiple panels need re-login simultaneously.
_browser_lock = threading.Semaphore(1)


def auto_login_panel(base_url, username, password, progress_cb=None):
    """
    Log into the SMS panel using a real browser with virtual display.

    Args:
        base_url : Panel base URL, e.g. "http://51.68.180.239/ints"
        username : Panel username
        password : Panel password
        progress_cb: Optional callable(str) for live status updates

    Returns:
        dict: {phpsessid, sesskey, data_url, referer, name, base_url}

    Raises:
        Exception on login failure or credential extraction failure.
    """

    def log(msg):
        logger.info(msg)
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    log("⏳ Waiting for browser slot (serialising logins)…")
    _browser_lock.acquire()
    log("🟢 Browser slot acquired — starting login")

    display = None
    driver = None

    try:
        # ── 0. Clean up any leftover browser processes ────────────────────────
        import subprocess, shutil as _shutil
        for _proc in ('chromedriver', 'chromium', 'chromium-browser', 'google-chrome'):
            try:
                subprocess.run(['pkill', '-f', _proc], capture_output=True, timeout=5)
            except Exception:
                pass
        time.sleep(1)

        # ── 1. Virtual display (Xvfb) ────────────────────────────────────────
        from pyvirtualdisplay import Display
        display = Display(visible=0, size=(1366, 768))
        display.start()
        log("🖥 Virtual display started")

        # ── 2. Browser setup ─────────────────────────────────────────────────
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        chromedriver_path = _shutil.which('chromedriver') or 'chromedriver'
        chromium_path = _shutil.which('chromium') or _shutil.which('chromium-browser') or 'chromium'

        def _make_driver():
            opts = Options()
            opts.binary_location = chromium_path
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--disable-software-rasterizer")
            opts.add_argument("--window-size=1366,768")
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_argument("--disable-extensions")
            opts.add_argument("--disable-background-networking")
            opts.add_argument("--disable-default-apps")
            opts.add_argument("--no-first-run")
            opts.add_argument("--no-zygote")
            opts.add_argument("--disable-setuid-sandbox")
            opts.add_argument("--disable-crash-reporter")
            opts.add_argument("--memory-pressure-off")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)
            svc = Service(executable_path=chromedriver_path)
            d = webdriver.Chrome(service=svc, options=opts)
            d.set_page_load_timeout(30)
            d.set_script_timeout(15)
            return d

        # Retry driver creation up to 3 times on session/crash errors
        for _attempt in range(1, 4):
            try:
                driver = _make_driver()
                time.sleep(2)  # brief pause to let browser stabilise
                break
            except Exception as _e:
                log(f"⚠️ Browser start attempt {_attempt}/3 failed: {_e}")
                try:
                    if driver:
                        driver.quit()
                except Exception:
                    pass
                driver = None
                for _proc in ('chromedriver', 'chromium'):
                    try:
                        subprocess.run(['pkill', '-f', _proc], capture_output=True, timeout=5)
                    except Exception:
                        pass
                if _attempt == 3:
                    raise
                time.sleep(3)

        # Mask automation flag
        try:
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
            })
        except Exception:
            pass
        log("🌐 Browser started (Chromium)")

        # ── 3. Navigate to login ─────────────────────────────────────────────
        login_url = base_url.rstrip('/') + '/login'
        driver.get(login_url)
        log(f"🔑 Login page loaded")
        time.sleep(4)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )

        username_field = driver.find_element(By.NAME, "username")
        password_field = driver.find_element(By.NAME, "password")
        username_field.clear()
        password_field.clear()
        username_field.send_keys(username)
        password_field.send_keys(password)
        log("✏️ Credentials entered")
        time.sleep(2)

        # ── 4. CAPTCHA (math: What is X + Y?) ───────────────────────────────
        page_src = driver.page_source
        captcha_match = re.search(r'What is\s*(\d+)\s*\+\s*(\d+)', page_src)

        captcha_field = None
        if captcha_match:
            num1 = int(captcha_match.group(1))
            num2 = int(captcha_match.group(2))
            answer = num1 + num2
            log(f"🧮 CAPTCHA: {num1} + {num2} = {answer}")

            for selector in ["input[name='capt']", "input[name='captcha']"]:
                try:
                    f = driver.find_element(By.CSS_SELECTOR, selector)
                    if f.is_displayed():
                        captcha_field = f
                        break
                except Exception:
                    continue

            if not captcha_field:
                for inp in driver.find_elements(By.TAG_NAME, "input"):
                    if inp.is_displayed() and inp.get_attribute("type") in ("text", "number"):
                        field_name = inp.get_attribute("name") or ""
                        if "username" not in field_name and "password" not in field_name:
                            captcha_field = inp
                            break

            if captcha_field:
                captcha_field.clear()
                captcha_field.send_keys(str(answer))
                log("✅ CAPTCHA answer filled")
            else:
                log("⚠️ CAPTCHA field not found — submitting without it")

        # ── 5. Submit the login form ─────────────────────────────────────────
        # Priority: explicit submit button → JS submit → Enter key fallback
        submitted = False

        # Method A: find and click the submit / login button
        submit_selectors = [
            "button[type='submit']",
            "input[type='submit']",
            "button.btn-primary",
            "button.btn-login",
            "button.btn-success",
            ".btn[type='submit']",
            "form button",
        ]
        for sel in submit_selectors:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, sel)
                if btn.is_displayed() and btn.is_enabled():
                    driver.execute_script("arguments[0].click();", btn)
                    log(f"🖱 Submit button clicked ({sel})")
                    submitted = True
                    break
            except Exception:
                continue

        # Method B: JS form.submit()
        if not submitted:
            try:
                driver.execute_script(
                    "var f = document.querySelector('form'); if(f) f.submit();"
                )
                log("📤 Form submitted via JavaScript")
                submitted = True
            except Exception:
                pass

        # Method C: Enter key on whichever field was last filled
        if not submitted:
            last_field = captcha_field or password_field
            try:
                last_field.send_keys(Keys.RETURN)
                log("⌨️ Form submitted via Enter key")
                submitted = True
            except Exception:
                pass

        if not submitted:
            raise Exception("Could not submit the login form — no submit button found and JS submit failed.")

        # ── 6. Wait for navigation away from login page ──────────────────────
        try:
            WebDriverWait(driver, 15).until(
                lambda d: "login" not in d.current_url.lower()
            )
            log("✅ Navigated away from login page")
        except Exception:
            pass  # will be caught by the URL check below

        current_url = driver.current_url
        log(f"🌐 Post-login URL: {current_url}")
        if "login" in current_url.lower():
            # Capture any error message shown on the page to help diagnose
            error_hint = ""
            try:
                for sel in [".alert", ".error", ".text-danger", "#error", ".login-error"]:
                    try:
                        el = driver.find_element(By.CSS_SELECTOR, sel)
                        txt = el.text.strip()
                        if txt:
                            error_hint = f" Panel says: \"{txt[:120]}\""
                            break
                    except Exception:
                        continue
            except Exception:
                pass
            raise Exception(
                f"Login failed — still on login page after submission.{error_hint} "
                f"Check username/password in Edit API → Edit Username & Password."
            )
        log("✅ Login successful!")

        # ── 6. Extract PHPSESSID ─────────────────────────────────────────────
        cookies = driver.get_cookies()
        phpsessid = next((c['value'] for c in cookies if c['name'] == 'PHPSESSID'), '')
        if not phpsessid:
            raise Exception("PHPSESSID not found in browser cookies after login.")
        log(f"🍪 PHPSESSID: {phpsessid[:8]}...")

        # ── 7. Navigate to SMS CDR reports and capture sesskey ───────────────
        parsed = urlparse(base_url)
        host_base = base_url.rstrip('/')
        referer_url = host_base + '/agent/SMSCDRReports'
        data_url = host_base + '/agent/res/data_smscdr.php'

        driver.get(referer_url)
        log("📊 Loaded SMS CDR Reports page — waiting for AJAX...")
        time.sleep(10)

        sesskey = ''

        # Method A: scan performance resource entries for data_smscdr.php URL
        try:
            urls_js = driver.execute_script(
                "return window.performance.getEntriesByType('resource')"
                "  .map(e => e.name)"
                "  .filter(n => n.indexOf('data_smscdr') > -1);"
            )
            for url in (urls_js or []):
                m = re.search(r'sesskey=([^&]+)', url)
                if m:
                    sesskey = m.group(1)
                    log(f"🔑 Sesskey from network log: {sesskey[:10]}...")
                    break
        except Exception:
            pass

        # Method B: search page source for sesskey value
        if not sesskey:
            page_src2 = driver.page_source
            for pattern in [
                r'["\']sesskey["\']\s*[:=]\s*["\']([^"\']{4,})["\']',
                r'sesskey=([A-Za-z0-9+/=]{4,})',
                r'var\s+sesskey\s*=\s*["\']([^"\']{4,})["\']',
                r'data\s*[:{]\s*["\']?sesskey["\']?\s*:\s*["\']([^"\']{4,})["\']',
            ]:
                m = re.search(pattern, page_src2)
                if m:
                    sesskey = m.group(1)
                    log(f"🔑 Sesskey from page source: {sesskey[:10]}...")
                    break

        # Method C: trigger the AJAX request directly from the browser and capture sesskey
        if not sesskey:
            try:
                today = time.strftime('%Y-%m-%d')
                ajax_url = (
                    f"{data_url}?fdate1={today}+00%3A00%3A00&fdate2={today}+23%3A59%3A59"
                    f"&fg=0&sEcho=1&iColumns=9&sColumns=%2C%2C%2C%2C%2C%2C%2C%2C"
                    f"&iDisplayStart=0&iDisplayLength=5"
                    f"&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3"
                    f"&mDataProp_4=4&mDataProp_5=5&mDataProp_6=6&mDataProp_7=7&mDataProp_8=8"
                    f"&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=desc&iSortingCols=1"
                    f"&_={int(time.time()*1000)}"
                )
                result_js = driver.execute_script(
                    f"var r = null; "
                    f"var xhr = new XMLHttpRequest(); "
                    f"xhr.open('GET', '{ajax_url}', false); "
                    f"xhr.send(); "
                    f"r = xhr.responseURL; "
                    f"return r;"
                )
                if result_js:
                    m = re.search(r'sesskey=([^&]+)', str(result_js))
                    if m:
                        sesskey = m.group(1)
                        log(f"🔑 Sesskey from AJAX trigger: {sesskey[:10]}...")
            except Exception:
                pass

        # Method D: check performance resources again after manual trigger
        if not sesskey:
            try:
                time.sleep(3)
                urls_js2 = driver.execute_script(
                    "return window.performance.getEntriesByType('resource')"
                    "  .map(e => e.name)"
                    "  .filter(n => n.indexOf('data_smscdr') > -1);"
                )
                for url in (urls_js2 or []):
                    m = re.search(r'sesskey=([^&]+)', url)
                    if m:
                        sesskey = m.group(1)
                        log(f"🔑 Sesskey from delayed network log: {sesskey[:10]}...")
                        break
            except Exception:
                pass

        if sesskey:
            log(f"✅ All credentials extracted successfully")
        else:
            log(f"⚠️ Sesskey not found — PHPSESSID extracted but sesskey missing (bot will still work without it)")

        hostname = parsed.hostname or host_base
        return {
            'phpsessid': phpsessid,
            'sesskey': sesskey,
            'data_url': data_url,
            'referer': referer_url,
            'name': f'Panel {hostname}',
            'base_url': base_url,
        }

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
            log("🔒 Browser closed")
        if display:
            try:
                display.stop()
            except Exception:
                pass
            log("🖥 Virtual display stopped")
        _browser_lock.release()
        log("🟢 Browser slot released")
