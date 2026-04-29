import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

# Load dotenv to catch credentials
from dotenv import load_dotenv
load_dotenv()

import customtkinter as ctk
import pandas as pd
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from webdriver_manager.microsoft import EdgeChromiumDriverManager

try:
    import winreg
except ImportError:
    winreg = None

APP_TITLE = "EMIS Grade Parser"
APP_CONFIG_PATH = Path("emis_config.json")
URL = "https://e-diary.emis.am/"
AUTH_WAIT_TIMEOUT_SEC = 15 * 60
STATUS_POLL_INTERVAL_SEC = 1.0

def load_app_config() -> dict:
    if APP_CONFIG_PATH.exists():
        try:
            with open(APP_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_app_config(config: dict):
    try:
        with open(APP_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
    except Exception:
        pass

SEMESTER_MAP = {
    "1": ["9", "10", "11", "12"],
    "2": ["1", "2", "3", "4", "5", "6"],
}

MONTH_NAMES_ARM = {
    "1": "Հունվար",
    "2": "Փետրվար",
    "3": "Մարտ",
    "4": "Ապրիլ",
    "5": "Մայիս",
    "6": "Հունիս",
    "9": "Սեպտեմբեր",
    "10": "Հոկտեմբեր",
    "11": "Նոյեմբեր",
    "12": "Դեկտեմբեր",
}

WINDOWS_BROWSER_PATHS = {
    "edge": [
        Path(os.environ.get("PROGRAMFILES", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe",
    ],
    "chrome": [
        Path(os.environ.get("PROGRAMFILES", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
    ],
    "brave": [
        Path(os.environ.get("PROGRAMFILES", "")) / "BraveSoftware" / "Brave-Browser" / "Application" / "brave.exe",
        Path(os.environ.get("PROGRAMFILES(X86)", "")) / "BraveSoftware" / "Brave-Browser" / "Application" / "brave.exe",
        Path(os.environ.get("LOCALAPPDATA", "")) / "BraveSoftware" / "Brave-Browser" / "Application" / "brave.exe",
    ],
}

BROWSER_PROGID_MAP = {
    "MSEdgeHTM": "edge",
    "ChromeHTML": "chrome",
    "BraveHTML": "brave",
}


@dataclass
class ParseConfig:
    login: str
    password: str
    browser: str
    browser_binary: str | None
    user_data_path: str | None
    profile_directory: str
    auth_timeout_sec: int = AUTH_WAIT_TIMEOUT_SEC


class ParseLogger:
    def __init__(self, out_queue: queue.Queue):
        self._queue = out_queue

    def log(self, message: str):
        stamp = datetime.now().strftime("%H:%M:%S")
        self._queue.put(("log", f"[{stamp}] {message}"))

    def progress(self, value: float):
        value = max(0.0, min(1.0, value))
        self._queue.put(("progress", value))


class ConsoleLogger:
    def log(self, message: str):
        stamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{stamp}] {message}")

    @staticmethod
    def progress(value: float):
        pct = max(0.0, min(1.0, value)) * 100
        print(f"[progress] {pct:.1f}%")


class BrowserResolver:
    @staticmethod
    def _default_browser_from_registry() -> str | None:
        if winreg is None:
            return None

        try:
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\Shell\Associations\UrlAssociations\http\UserChoice",
            ) as key:
                prog_id = winreg.QueryValueEx(key, "ProgId")[0]

            for prefix, browser in BROWSER_PROGID_MAP.items():
                if prog_id.startswith(prefix):
                    return browser
        except OSError:
            return None

        return None

    @staticmethod
    def _find_binary(browser: str) -> str | None:
        if browser not in WINDOWS_BROWSER_PATHS:
            return None

        for candidate in WINDOWS_BROWSER_PATHS[browser]:
            if str(candidate) and candidate.exists():
                return str(candidate)

        which_map = {
            "edge": ["msedge", "microsoft-edge"],
            "chrome": ["chrome", "google-chrome", "chrome.exe"],
            "brave": ["brave", "brave-browser", "brave.exe"],
        }
        for cmd in which_map.get(browser, []):
            found = shutil.which(cmd)
            if found:
                return found

        return None

    @classmethod
    def resolve(cls) -> tuple[str, str | None, dict[str, str | None]]:
        installed = {
            "edge": cls._find_binary("edge"),
            "chrome": cls._find_binary("chrome"),
            "brave": cls._find_binary("brave"),
        }

        default = cls._default_browser_from_registry()
        if default and installed.get(default):
            return default, installed[default], installed

        for browser in ("edge", "chrome", "brave"):
            if installed.get(browser):
                return browser, installed[browser], installed

        return "edge", None, installed


class ReportBuilder:
    @staticmethod
    def custom_round(val):
        if pd.isna(val):
            return 0
        return int(val + 0.5)

    @classmethod
    def generate_report_df(cls, df_source):
        if df_source.empty:
            return pd.DataFrame(), 0

        df_g = df_source[df_source["Grade"] != "բ"].copy()
        df_g["Grade"] = pd.to_numeric(df_g["Grade"])

        df_abs = df_source[df_source["Grade"] == "բ"]
        total_abs = len(df_abs)

        stats = df_g.groupby("Subject")["Grade"].agg(["count", "mean"])
        stats.columns = ["Count", "Average"]
        stats["Final"] = stats["Average"].apply(cls.custom_round)

        abs_counts = df_abs.groupby("Subject")["Grade"].count()
        abs_counts.name = "Absences"

        res = stats.join(abs_counts, how="outer").fillna(0)
        res["Absences"] = res["Absences"].astype(int)
        res["Count"] = res["Count"].astype(int)

        sorted_res = res.sort_values(by=["Average", "Absences"], ascending=False)
        return sorted_res, total_abs

    @staticmethod
    def calculate_total_final_score(report_df, divider=14):
        if report_df.empty or "Final" not in report_df.columns:
            return 0
        return report_df["Final"].sum() / divider

    @classmethod
    def build_bundle(cls, data):
        df = pd.DataFrame(data)

        df1 = df[df["Semester"] == 1] if not df.empty else pd.DataFrame()
        df2 = df[df["Semester"] == 2] if not df.empty else pd.DataFrame()

        sem1_df, sem1_abs = cls.generate_report_df(df1)
        sem2_df, sem2_abs = cls.generate_report_df(df2)
        year_df, year_abs = cls.generate_report_df(df)

        sem1_score = cls.calculate_total_final_score(sem1_df)
        sem2_score = cls.calculate_total_final_score(sem2_df)
        year_score = cls.calculate_total_final_score(year_df)

        return {
            "raw": df,
            "sem1_df": sem1_df,
            "sem2_df": sem2_df,
            "year_df": year_df,
            "sem1_abs": sem1_abs,
            "sem2_abs": sem2_abs,
            "year_abs": year_abs,
            "sem1_score": sem1_score,
            "sem2_score": sem2_score,
            "year_score": year_score,
        }


class Exporter:
    @staticmethod
    def export_excel(bundle, file_path: str):
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            if not bundle["sem1_df"].empty:
                bundle["sem1_df"].to_excel(writer, sheet_name="Sem_1")
            if not bundle["sem2_df"].empty:
                bundle["sem2_df"].to_excel(writer, sheet_name="Sem_2")
            bundle["year_df"].to_excel(writer, sheet_name="Year_Total")
            bundle["raw"].to_excel(writer, sheet_name="Raw", index=False)

    @staticmethod
    def _resolve_unicode_font_path() -> str | None:
        windows_font = Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts" / "segoeui.ttf"
        if windows_font.exists():
            return str(windows_font)

        for candidate in (
            Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            Path("/Library/Fonts/Arial Unicode.ttf"),
            Path("/Library/Fonts/Arial Unicode MS.ttf"),
        ):
            if candidate.exists():
                return str(candidate)

        return None

    @staticmethod
    def export_pdf(bundle, file_path: str):
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=12)
        pdf.add_page()

        unicode_font = Exporter._resolve_unicode_font_path()
        unicode_supported = False
        if unicode_font:
            try:
                pdf.add_font("UI", style="", fname=unicode_font)
                pdf.set_font("UI", size=11)
                unicode_supported = True
            except Exception:
                pdf.set_font("Helvetica", size=11)
        else:
            pdf.set_font("Helvetica", size=11)

        def safe(text: str) -> str:
            if unicode_supported:
                return text
            return text.encode("latin-1", errors="replace").decode("latin-1")

        pdf.set_font_size(16)
        pdf.cell(0, 10, safe("EMIS Grade Report"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font_size(10)
        pdf.cell(
            0,
            8,
            safe(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"),
            new_x=XPos.LMARGIN,
            new_y=YPos.NEXT,
        )
        pdf.ln(3)

        def write_section(title, report_df, total_abs, total_score):
            pdf.set_font_size(13)
            pdf.cell(0, 8, safe(title), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.set_font_size(10)
            if report_df.empty:
                pdf.cell(0, 6, safe("No data"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            else:
                pdf.cell(
                    0,
                    6,
                    safe("Subject | Count | Average | Final | Absences"),
                    new_x=XPos.LMARGIN,
                    new_y=YPos.NEXT,
                )
                pdf.set_font_size(9)
                for subject, row in report_df.iterrows():
                    subj = str(subject)
                    if len(subj) > 44:
                        subj = subj[:41] + "..."
                    line = (
                        f"{subj:<44} | {int(row['Count']):>3} | {row['Average']:>6.2f}"
                        f" | {int(row['Final']):>3} | {int(row['Absences']):>3}"
                    )
                    pdf.cell(0, 6, safe(line), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
                pdf.set_font_size(10)
            pdf.cell(0, 6, safe(f"TOTAL ABSENCES: {total_abs}"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.cell(
                0,
                6,
                safe(f"FINAL SCORE (sum(Final)/14): {total_score:.2f}"),
                new_x=XPos.LMARGIN,
                new_y=YPos.NEXT,
            )
            pdf.ln(3)

        write_section("SEM 1", bundle["sem1_df"], bundle["sem1_abs"], bundle["sem1_score"])
        write_section("SEM 2", bundle["sem2_df"], bundle["sem2_abs"], bundle["sem2_score"])
        write_section("YEAR TOTAL", bundle["year_df"], bundle["year_abs"], bundle["year_score"])

        pdf.output(file_path)


class EmisParserEngine:
    def __init__(self, config: ParseConfig, logger, stop_event: threading.Event):
        self.config = config
        self.logger = logger
        self.stop_event = stop_event

    def _check_cancelled(self):
        if self.stop_event.is_set():
            raise RuntimeError("Parsing cancelled by user")

    def _build_driver(self) -> webdriver.Remote:
        browser = self.config.browser
        if browser == "edge":

            def build_edge_options(use_profile: bool):
                options = EdgeOptions()
                options.page_load_strategy = "eager"
                options.add_argument("--start-maximized")
                options.add_argument("--log-level=3")
                options.add_argument("--silent")
                options.add_argument("--disable-dev-shm-usage")
                options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
                options.add_experimental_option("useAutomationExtension", False)

                if use_profile and self.config.user_data_path:
                    options.add_argument(f"--user-data-dir={self.config.user_data_path}")
                if use_profile and self.config.profile_directory:
                    options.add_argument(f"--profile-directory={self.config.profile_directory}")
                if self.config.browser_binary:
                    options.binary_location = self.config.browser_binary
                return options

            manager_failed = False
            service = None
            try:
                manager_path = EdgeChromiumDriverManager().install()
                service = EdgeService(manager_path)
                self.logger.log("Edge driver resolved by webdriver-manager")
            except Exception as exc:
                manager_failed = True
                self.logger.log(f"webdriver-manager failed for Edge ({exc}). Falling back to Selenium Manager")

            last_error = None
            for use_profile in (True, False):
                if not use_profile:
                    self.logger.log("Retrying browser startup without user profile lock")

                options = build_edge_options(use_profile)
                try:
                    if not manager_failed and service:
                        return webdriver.Edge(service=service, options=options)
                    else:
                        return webdriver.Edge(options=options)
                except WebDriverException as exc:
                    last_error = exc
                    continue

            raise last_error if last_error else RuntimeError("Unable to start Edge WebDriver")

        if browser in ("chrome", "brave"):

            def build_chrome_options(use_profile: bool):
                options = ChromeOptions()
                options.page_load_strategy = "eager"
                options.add_argument("--start-maximized")
                options.add_argument("--log-level=3")
                options.add_argument("--disable-dev-shm-usage")
                options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
                options.add_experimental_option("useAutomationExtension", False)

                if use_profile and self.config.user_data_path:
                    options.add_argument(f"--user-data-dir={self.config.user_data_path}")
                if use_profile and self.config.profile_directory:
                    options.add_argument(f"--profile-directory={self.config.profile_directory}")
                if self.config.browser_binary:
                    options.binary_location = self.config.browser_binary
                return options

            chrome_type = ChromeType.BRAVE if browser == "brave" else ChromeType.GOOGLE
            manager_failed = False
            service = None
            try:
                manager_path = ChromeDriverManager(chrome_type=chrome_type).install()
                service = ChromeService(manager_path)
                self.logger.log(f"{browser.title()} driver resolved by webdriver-manager")
            except Exception as exc:
                manager_failed = True
                self.logger.log(
                    f"webdriver-manager failed for {browser} ({exc}). Falling back to Selenium Manager"
                )

            last_error = None
            for use_profile in (True, False):
                if not use_profile:
                    self.logger.log("Retrying browser startup without user profile lock")

                options = build_chrome_options(use_profile)
                try:
                    if not manager_failed and service:
                        return webdriver.Chrome(service=service, options=options)
                    else:
                        return webdriver.Chrome(options=options)
                except WebDriverException as exc:
                    last_error = exc
                    continue

            raise last_error if last_error else RuntimeError(f"Unable to start {browser} WebDriver")

        raise ValueError(f"Unsupported browser: {browser}")

    @staticmethod
    def _format_duration(seconds):
        seconds = max(0, int(seconds))
        minutes, secs = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"

    @staticmethod
    def _is_authorized(driver):
        if driver.find_elements(By.NAME, "semester"):
            return True

        if driver.find_elements(By.CSS_SELECTOR, ".btn.btn-sm.btn-primary.btn-outline-primary"):
            return True

        current_url = driver.current_url.lower()
        return "/diary/" in current_url or "/children" in current_url

    def _wait_for_authorization(self, driver, deadline):
        last_notice = 0.0

        while time.monotonic() < deadline:
            self._check_cancelled()

            if self._is_authorized(driver):
                return True

            now = time.monotonic()
            if now - last_notice >= 15:
                remaining = deadline - now
                self.logger.log(
                    f"Waiting for manual login / robot verification. Remaining {self._format_duration(remaining)}"
                )
                last_notice = now

            time.sleep(STATUS_POLL_INTERVAL_SEC)

        return False

    def _auto_login(self, driver, deadline):
        username_fields = driver.find_elements(By.NAME, "email")
        login_form_present = bool(username_fields)

        if login_form_present:
            self.logger.log("Login form detected")
            if self.config.login and self.config.password:
                username_fields[0].clear()
                username_fields[0].send_keys(self.config.login)
                password_field = driver.find_element(By.NAME, "password")
                password_field.clear()
                password_field.send_keys(self.config.password)
                submit_btns = driver.find_elements(By.CSS_SELECTOR, "button[type='submit']")
                if submit_btns:
                    submit_btns[0].click()
                    self.logger.log("Credentials submitted. Complete robot check if required")
            else:
                self.logger.log("No credentials provided. Please log in manually in opened browser")
        else:
            self.logger.log("Existing authenticated session detected")

        if not self._wait_for_authorization(driver, deadline):
            raise TimeoutError(
                f"Authorization did not finish within {self.config.auth_timeout_sec // 60} minutes"
            )

        diary_selectors = [
            ".btn.btn-sm.btn-primary.btn-outline-primary",
            "a[href*='diary']",
            "a[href*='journal']",
        ]
        for selector in diary_selectors:
            buttons = driver.find_elements(By.CSS_SELECTOR, selector)
            if buttons:
                driver.execute_script("arguments[0].click();", buttons[0])
                self.logger.log("Diary entry point opened")
                break

    @staticmethod
    def _default_user_profile_path(browser: str) -> str | None:
        localapp = Path(os.environ.get("LOCALAPPDATA", ""))
        if not localapp:
            return None

        profile_map = {
            "edge": localapp / "Microsoft" / "Edge" / "User Data",
            "chrome": localapp / "Google" / "Chrome" / "User Data",
            "brave": localapp / "BraveSoftware" / "Brave-Browser" / "User Data",
        }
        path = profile_map.get(browser)
        if path and path.exists():
            return str(path)
        return None

    @staticmethod
    def _wait_for_week_buttons(driver, timeout=3):
        try:
            return WebDriverWait(driver, timeout).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "a[data-week]")
            )
        except Exception:
            return []

    def _wait_for_badges_stable(self, driver, timeout=2.0, poll_interval=0.10):
        start = time.perf_counter()
        stable_hits = 0
        last_count = -1

        while time.perf_counter() - start < timeout:
            self._check_cancelled()
            badges = driver.find_elements(By.CSS_SELECTOR, "label.badge")
            current_count = len(badges)

            if current_count == last_count:
                stable_hits += 1
            else:
                stable_hits = 0
                last_count = current_count

            if stable_hits >= 2:
                return badges

            time.sleep(poll_interval)

        return driver.find_elements(By.CSS_SELECTOR, "label.badge")

    def parse_grades(self):
        started_at = time.perf_counter()
        deadline = time.monotonic() + self.config.auth_timeout_sec
        weeks_processed = 0
        weeks_total = 0
        all_data = []
        driver = None

        try:
            self.logger.progress(0.02)
            self.logger.log(f"Starting browser: {self.config.browser}")
            driver = self._build_driver()

            wait = WebDriverWait(driver, 10)
            self._check_cancelled()

            self.logger.log(f"Loading {URL}")
            driver.get(URL)
            self.logger.progress(0.05)

            self._auto_login(driver, deadline)
            self.logger.progress(0.1)

            self.logger.log("Waiting for semester selector")
            wait.until(EC.presence_of_element_located((By.NAME, "semester")))
            sem_select = driver.find_element(By.NAME, "semester")
            sems = [o.get_attribute("value") for o in Select(sem_select).options if o.get_attribute("value")]
            self.logger.log(f"Available semesters: {sems}")

            future_limit = datetime.now() + timedelta(days=1)

            for sem in sems:
                self._check_cancelled()
                if sem not in SEMESTER_MAP:
                    continue

                self.logger.log(f"SEM {sem} start")
                Select(driver.find_element(By.NAME, "semester")).select_by_value(sem)
                time.sleep(0.25)

                for m_val in SEMESTER_MAP[sem]:
                    self._check_cancelled()
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"Parsing stopped after {self.config.auth_timeout_sec // 60} minutes"
                        )

                    month_name = MONTH_NAMES_ARM.get(m_val, m_val)
                    self.logger.log(f"Month {month_name}")

                    Select(driver.find_element(By.NAME, "month")).select_by_value(m_val)
                    submit = driver.find_element(By.CLASS_NAME, "diary_submit")
                    driver.execute_script("arguments[0].click();", submit)

                    week_btns = self._wait_for_week_buttons(driver, timeout=3)
                    if not week_btns:
                        self.logger.log(f"Month {month_name}: no weeks found")
                        continue

                    week_values = [
                        btn.get_attribute("data-week")
                        for btn in week_btns
                        if btn.get_attribute("data-week")
                    ]
                    if not week_values:
                        self.logger.log(f"Month {month_name}: no week ids found")
                        continue

                    weeks_total += len(week_values)

                    try:
                        first_week_start = datetime.strptime(
                            week_btns[0].text.strip().split(" - ")[0], "%d.%m.%Y"
                        )
                        if first_week_start > future_limit:
                            self.logger.log(f"Month {month_name}: future-only, skipping")
                            break
                    except Exception:
                        pass

                    for week_index, week_id in enumerate(week_values, start=1):
                        self._check_cancelled()
                        if time.monotonic() >= deadline:
                            raise TimeoutError(
                                f"Parsing stopped after {self.config.auth_timeout_sec // 60} minutes"
                            )

                        candidates = driver.find_elements(By.CSS_SELECTOR, f"a[data-week='{week_id}']")
                        if not candidates:
                            continue

                        curr = candidates[0]
                        week_text = curr.text.strip()

                        try:
                            w_start = datetime.strptime(week_text.split(" - ")[0], "%d.%m.%Y")
                            if w_start > future_limit:
                                break
                        except Exception:
                            pass

                        self.logger.log(f"Week {week_index}/{len(week_values)} {week_text}")
                        driver.execute_script("arguments[0].click();", curr)
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", curr)
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                        badges = self._wait_for_badges_stable(driver, timeout=2.0, poll_interval=0.10)

                        weeks_processed += 1
                        elapsed = time.perf_counter() - started_at
                        avg_per_week = elapsed / weeks_processed if weeks_processed else 0
                        remaining = max(weeks_total - weeks_processed, 0)
                        eta = remaining * avg_per_week
                        self.logger.log(
                            f"Week ready in {avg_per_week:.2f}s avg, ETA {self._format_duration(eta)}"
                        )

                        if weeks_total > 0:
                            self.logger.progress(min(0.1 + 0.8 * (weeks_processed / weeks_total), 0.95))

                        for badge in badges:
                            txt = badge.text.strip()
                            if txt.isdigit() or txt == "բ":
                                try:
                                    subject = (
                                        badge.find_element(By.XPATH, "./ancestor::h3")
                                        .text.replace(txt, "")
                                        .strip()
                                    )
                                    value = int(txt) if txt.isdigit() else "բ"
                                    self.logger.log(f"    {subject}. {value}")
                                    all_data.append(
                                        {
                                            "Semester": int(sem),
                                            "Month": month_name,
                                            "Subject": subject,
                                            "Grade": value,
                                        }
                                    )
                                except Exception:
                                    continue

                    self.logger.log(f"Month {month_name} done")

                self.logger.log(f"SEM {sem} done")

            total_time = time.perf_counter() - started_at
            self.logger.log(f"TOTAL PARSE TIME: {total_time:.2f} sec")
            if weeks_processed:
                self.logger.log(f"AVERAGE TIME PER WEEK: {total_time / weeks_processed:.2f} sec")

            self.logger.progress(1.0)
            return all_data

        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass


class EmisGuiApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1200x860")
        self.minsize(1040, 780)

        self.app_config = load_app_config()

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        # More modern/ergonomic font configuration
        self.font_title = ctk.CTkFont(family="Segoe UI", size=28, weight="bold")
        self.font_subtitle = ctk.CTkFont(family="Segoe UI", size=15, weight="bold")
        self.font_body = ctk.CTkFont(family="Segoe UI", size=13)
        self.font_mono = ctk.CTkFont(family="Consolas", size=12)

        self.event_queue = queue.Queue()
        self.worker_thread = None
        self.stop_event = threading.Event()

        self.parsed_data = []
        self.report_bundle = None

        self.detected_browser, _, self.installed_browsers = BrowserResolver.resolve()

        self._build_ui()
        self.after(100, self._process_queue)

        self._append_log("Application started")
        self._append_log(f"Default browser detected: {self.detected_browser}")

        if os.environ.get("EMIS_GUI_SMOKE") == "1":
            self.after(600, self.start_parsing)

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        header = ctk.CTkFrame(self, corner_radius=16)
        header.grid(row=0, column=0, padx=18, pady=(18, 10), sticky="ew")
        header.grid_columnconfigure((0, 1, 2, 3), weight=1)

        ctk.CTkLabel(header, text=APP_TITLE, font=self.font_title).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=16, pady=(14, 8)
        )

        ctk.CTkLabel(header, text="Login", font=self.font_body).grid(row=1, column=0, sticky="w", padx=16)
        self.login_entry = ctk.CTkEntry(header, placeholder_text="EMIS login", corner_radius=10, height=36)
        login_val = self.app_config.get("login", "")
        if not login_val:
            login_val = os.environ.get("email", "")
        self.login_entry.insert(0, login_val)
        self.login_entry.grid(row=2, column=0, sticky="ew", padx=16, pady=(4, 12))

        ctk.CTkLabel(header, text="Password", font=self.font_body).grid(row=1, column=1, sticky="w", padx=16)
        self.password_entry = ctk.CTkEntry(
            header,
            placeholder_text="EMIS password",
            show="*",
            corner_radius=10,
            height=36,
        )
        pwd_val = self.app_config.get("password", "")
        if not pwd_val:
            pwd_val = os.environ.get("password", "")
        self.password_entry.insert(0, pwd_val)
        self.password_entry.grid(row=2, column=1, sticky="ew", padx=16, pady=(4, 12))

        ctk.CTkLabel(header, text="Browser", font=self.font_body).grid(row=1, column=2, sticky="w", padx=16)
        self.browser_menu = ctk.CTkOptionMenu(
            header,
            values=["edge", "chrome", "brave"],
            corner_radius=10,
            height=36,
        )
        self.browser_menu.set(self.detected_browser)
        self.browser_menu.grid(row=2, column=2, sticky="ew", padx=16, pady=(4, 12))

        ctk.CTkLabel(header, text="Output base name", font=self.font_body).grid(row=1, column=3, sticky="w", padx=16)
        self.output_entry = ctk.CTkEntry(header, corner_radius=10, height=36)
        self.output_entry.insert(0, str(Path.cwd() / "Emis_Report"))
        self.output_entry.grid(row=2, column=3, sticky="ew", padx=16, pady=(4, 12))

        status = ctk.CTkFrame(self, corner_radius=16)
        status.grid(row=1, column=0, padx=18, pady=(0, 10), sticky="ew")
        status.grid_columnconfigure(0, weight=1)

        self.status_label = ctk.CTkLabel(status, text="Ready", font=self.font_subtitle, anchor="w")
        self.status_label.grid(row=0, column=0, sticky="ew", padx=16, pady=(12, 6))

        self.progress_bar = ctk.CTkProgressBar(status, corner_radius=10)
        self.progress_bar.set(0)
        self.progress_bar.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 12))

        actions = ctk.CTkFrame(self, corner_radius=16)
        actions.grid(row=2, column=0, padx=18, pady=(0, 10), sticky="ew")
        actions.grid_columnconfigure((0, 1, 2, 3), weight=1)

        self.start_btn = ctk.CTkButton(
            actions,
            text="Start Parsing",
            command=self.start_parsing,
            height=38,
            corner_radius=12,
            font=self.font_subtitle,
        )
        self.start_btn.grid(row=0, column=0, padx=10, pady=12, sticky="ew")

        self.stop_btn = ctk.CTkButton(
            actions,
            text="Stop",
            command=self.stop_parsing,
            state="disabled",
            height=38,
            corner_radius=12,
            fg_color="#8b1d3a",
            hover_color="#a51f45",
            font=self.font_subtitle,
        )
        self.stop_btn.grid(row=0, column=1, padx=10, pady=12, sticky="ew")

        self.export_excel_btn = ctk.CTkButton(
            actions,
            text="Export to Excel",
            command=self.export_excel,
            state="disabled",
            height=38,
            corner_radius=12,
            font=self.font_subtitle,
        )
        self.export_excel_btn.grid(row=0, column=2, padx=10, pady=12, sticky="ew")

        self.export_pdf_btn = ctk.CTkButton(
            actions,
            text="Export to PDF",
            command=self.export_pdf,
            state="disabled",
            height=38,
            corner_radius=12,
            font=self.font_subtitle,
        )
        self.export_pdf_btn.grid(row=0, column=3, padx=10, pady=12, sticky="ew")

        content = ctk.CTkFrame(self, corner_radius=16)
        content.grid(row=3, column=0, padx=18, pady=(0, 18), sticky="nsew")
        content.grid_columnconfigure((0, 1), weight=1)
        content.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(content, text="Live Logs", font=self.font_subtitle).grid(
            row=0, column=0, sticky="w", padx=14, pady=(12, 6)
        )
        ctk.CTkLabel(content, text="Dashboard / Results", font=self.font_subtitle).grid(
            row=0, column=1, sticky="w", padx=14, pady=(12, 6)
        )

        self.log_box = ctk.CTkTextbox(content, corner_radius=10, font=self.font_mono)
        self.log_box.grid(row=1, column=0, sticky="nsew", padx=(14, 8), pady=(0, 14))

        right = ctk.CTkFrame(content, corner_radius=10)
        right.grid(row=1, column=1, sticky="nsew", padx=(8, 14), pady=(0, 14))
        right.grid_rowconfigure(2, weight=1)
        right.grid_columnconfigure(0, weight=1)

        self.summary_label = ctk.CTkLabel(
            right,
            text="Run parser to see statistics",
            justify="left",
            anchor="w",
            font=self.font_body,
        )
        self.summary_label.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 8))

        self.tabview = ctk.CTkTabview(right, corner_radius=10)
        self.tabview.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))

        self.tab_sem1 = self.tabview.add("Sem_1")
        self.tab_sem2 = self.tabview.add("Sem_2")
        self.tab_year = self.tabview.add("Year_Total")
        self.tab_raw = self.tabview.add("Raw")
        self.tab_target = self.tabview.add("Target Grade")

        for tab in (self.tab_sem1, self.tab_sem2, self.tab_year, self.tab_raw):
            tab.grid_rowconfigure(0, weight=1)
            tab.grid_columnconfigure(0, weight=1)

        self.sem1_box = ctk.CTkTextbox(self.tab_sem1, font=self.font_mono)
        self.sem2_box = ctk.CTkTextbox(self.tab_sem2, font=self.font_mono)
        self.year_box = ctk.CTkTextbox(self.tab_year, font=self.font_mono)
        self.raw_box = ctk.CTkTextbox(self.tab_raw, font=self.font_mono)

        self.sem1_box.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self.sem2_box.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self.year_box.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self.raw_box.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        self._build_target_tab()

    def _build_target_tab(self):
        self.tab_target.grid_columnconfigure(0, weight=1)
        self.tab_target.grid_rowconfigure(2, weight=1)

        options_frame = ctk.CTkFrame(self.tab_target, corner_radius=10, fg_color="transparent")
        options_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        options_frame.grid_columnconfigure((0, 1, 2), weight=1)

        ctk.CTkLabel(options_frame, text="Semester", font=self.font_body).grid(row=0, column=0, sticky="w")
        self.calc_sem_menu = ctk.CTkOptionMenu(
            options_frame,
            values=["Sem 1", "Sem 2", "Year"],
            command=self._on_calc_sem_change,
            height=32
        )
        self.calc_sem_menu.grid(row=1, column=0, sticky="ew", padx=(0, 10), pady=(4, 0))

        ctk.CTkLabel(options_frame, text="Subject", font=self.font_body).grid(row=0, column=1, sticky="w")
        self.calc_subject_menu = ctk.CTkOptionMenu(
            options_frame,
            values=["-"],
            command=self._on_calc_subj_change,
            state="disabled",
            height=32
        )
        self.calc_subject_menu.grid(row=1, column=1, sticky="ew", padx=10, pady=(4, 0))

        ctk.CTkLabel(options_frame, text="Desired Final Grade", font=self.font_body).grid(row=0, column=2, sticky="w", padx=10)
        self.calc_target_menu = ctk.CTkOptionMenu(
            options_frame,
            values=[str(i) for i in range(5, 11)],
            command=self._on_calc_target_change,
            state="disabled",
            height=32
        )
        self.calc_target_menu.set("8")
        self.calc_target_menu.grid(row=1, column=2, sticky="ew", padx=10, pady=(4, 0))

        info_frame = ctk.CTkFrame(self.tab_target, corner_radius=8)
        info_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(10, 5))
        self.calc_current_label = ctk.CTkLabel(
            info_frame, text="Select subject to see current stats", font=self.font_body, anchor="w"
        )
        self.calc_current_label.pack(fill="x", padx=12, pady=12)

        self.calc_result_box = ctk.CTkTextbox(self.tab_target, corner_radius=10, font=self.font_body)
        self.calc_result_box.grid(row=2, column=0, sticky="nsew", padx=10, pady=10)

    def _on_calc_sem_change(self, value):
        self._populate_calc_subjects()

    def _on_calc_subj_change(self, value):
        self._update_calc()

    def _on_calc_target_change(self, value):
        self._update_calc()

    def _populate_calc_subjects(self):
        if not self.report_bundle:
            return

        sem_choice = self.calc_sem_menu.get()
        if sem_choice == "Sem 1":
            df = self.report_bundle.get("sem1_df")
        elif sem_choice == "Sem 2":
            df = self.report_bundle.get("sem2_df")
        else:
            df = self.report_bundle.get("year_df")

        if df is None or df.empty:
            self.calc_subject_menu.configure(values=["-"], state="disabled")
            self.calc_target_menu.configure(state="disabled")
            self.calc_subject_menu.set("-")
            self.calc_current_label.configure(text="No data available for this semester")
            self.calc_result_box.delete("1.0", "end")
            return

        subjects = df.index.tolist()
        self.calc_subject_menu.configure(values=subjects, state="normal")
        self.calc_target_menu.configure(state="normal")
        
        curr = self.calc_subject_menu.get()
        if curr not in subjects:
            self.calc_subject_menu.set(subjects[0])
        
        self._update_calc()

    def _update_calc(self):
        if not self.report_bundle:
            return

        sem_choice = self.calc_sem_menu.get()
        if sem_choice == "Sem 1":
            df = self.report_bundle.get("sem1_df")
        elif sem_choice == "Sem 2":
            df = self.report_bundle.get("sem2_df")
        else:
            df = self.report_bundle.get("year_df")

        subj = self.calc_subject_menu.get()
        if subj == "-" or not hasattr(df, "index") or subj not in df.index:
            return

        row = df.loc[subj]
        S = round(row["Average"] * row["Count"])  
        N = int(row["Count"])
        curr_avg = row["Average"]
        curr_final = row["Final"]
        target = float(self.calc_target_menu.get())

        self.calc_current_label.configure(
            text=f"Current: Average {curr_avg:.2f}  |  Final Grade: {curr_final}  |  (Total {N} grades)"
        )

        min_avg_needed = target - 0.5
        
        if curr_avg >= min_avg_needed:
            msg = f"You are already meeting the target for {int(target)}!\nNo need for extra grades."
            self.calc_result_box.delete("1.0", "end")
            self.calc_result_box.insert("1.0", msg)
            return
            
        msg = f"To raise your final grade from {curr_final} to {int(target)} (average >= {min_avg_needed}), you need:\n\n"
        found = False
        
        import math
        for v in [10, 9, 8, 7, 6, 5]:
            if v <= min_avg_needed:
                continue
            
            numerator = min_avg_needed * N - S
            denominator = v - min_avg_needed
            
            if denominator > 0:
                k = math.ceil(numerator / denominator)
                if k > 0:
                    msg += f"• {k} grade{'s' if k > 1 else ''} of '{v}'\n"
                    found = True
        
        if not found:
            msg += "It's mathematically too difficult with normal grades."
            
        self.calc_result_box.delete("1.0", "end")
        self.calc_result_box.insert("1.0", msg)

    def _append_log(self, text: str):
        self.log_box.insert("end", text + "\n")
        self.log_box.see("end")

    def _build_config(self):
        browser = self.browser_menu.get().strip().lower()
        binary = self.installed_browsers.get(browser)
        user_data = EmisParserEngine._default_user_profile_path(browser)
        return ParseConfig(
            login=self.login_entry.get().strip(),
            password=self.password_entry.get().strip(),
            browser=browser,
            browser_binary=binary,
            user_data_path=user_data,
            profile_directory="Default",
            auth_timeout_sec=AUTH_WAIT_TIMEOUT_SEC,
        )

    def _set_running_state(self, running: bool):
        self.start_btn.configure(state="disabled" if running else "normal")
        self.stop_btn.configure(state="normal" if running else "disabled")

    def start_parsing(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self._append_log("Worker is already running")
            return

        self.app_config["login"] = self.login_entry.get().strip()
        self.app_config["password"] = self.password_entry.get().strip()
        save_app_config(self.app_config)

        self.stop_event.clear()
        self.progress_bar.set(0)
        self.report_bundle = None
        self.parsed_data = []
        self.export_excel_btn.configure(state="disabled")
        self.export_pdf_btn.configure(state="disabled")

        self.status_label.configure(text="Running...")
        self._set_running_state(True)

        config = self._build_config()
        logger = ParseLogger(self.event_queue)

        def worker():
            try:
                engine = EmisParserEngine(config, logger, self.stop_event)
                data = engine.parse_grades()
                bundle = ReportBuilder.build_bundle(data)
                self.event_queue.put(("done", (data, bundle)))
            except Exception as exc:
                self.event_queue.put(("error", f"{exc}\n{traceback.format_exc()}"))

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def stop_parsing(self):
        self.stop_event.set()
        self._append_log("Stop requested by user")
        self.status_label.configure(text="Stopping...")
        self.stop_btn.configure(state="disabled")

    def _report_to_text(self, title: str, report_df: pd.DataFrame, total_abs: int, total_score: float) -> str:
        lines = [f"=== {title} ==="]
        if report_df.empty:
            lines.append("No data")
        else:
            lines.append(report_df.to_string())
        lines.append("")
        lines.append(f"TOTAL ABSENCES: {total_abs}")
        lines.append(f"{title} FINAL SCORE (sum(Final)/14): {total_score:.2f}")
        return "\n".join(lines)

    def _render_results(self, bundle):
        self.summary_label.configure(
            text=(
                f"SEM 1 absences: {bundle['sem1_abs']}\n"
                f"SEM 2 absences: {bundle['sem2_abs']}\n"
                f"YEAR absences: {bundle['year_abs']}\n"
                f"SEM 1 final score: {bundle['sem1_score']:.2f}\n"
                f"SEM 2 final score: {bundle['sem2_score']:.2f}\n"
                f"YEAR final score: {bundle['year_score']:.2f}"
            )
        )

        sem1_text = self._report_to_text("SEM 1", bundle["sem1_df"], bundle["sem1_abs"], bundle["sem1_score"])
        sem2_text = self._report_to_text("SEM 2", bundle["sem2_df"], bundle["sem2_abs"], bundle["sem2_score"])
        year_text = self._report_to_text(
            "YEAR TOTAL",
            bundle["year_df"],
            bundle["year_abs"],
            bundle["year_score"],
        )

        raw_df = bundle["raw"]
        raw_text = "=== RAW ===\n"
        raw_text += raw_df.to_string(index=False) if not raw_df.empty else "No data"

        for box, text in (
            (self.sem1_box, sem1_text),
            (self.sem2_box, sem2_text),
            (self.year_box, year_text),
            (self.raw_box, raw_text),
        ):
            box.delete("1.0", "end")
            box.insert("1.0", text)

    def _base_output_path(self) -> Path:
        raw = self.output_entry.get().strip()
        if not raw:
            raw = str(Path.cwd() / "Emis_Report")
        path = Path(raw)
        if path.suffix:
            return path.with_suffix("")
        return path

    def export_excel(self):
        if not self.report_bundle:
            self._append_log("No results to export")
            return

        file_path = str(self._base_output_path().with_suffix(".xlsx"))
        Exporter.export_excel(self.report_bundle, file_path)
        self._append_log(f"Excel exported: {file_path}")

    def export_pdf(self):
        if not self.report_bundle:
            self._append_log("No results to export")
            return

        file_path = str(self._base_output_path().with_suffix(".pdf"))
        try:
            Exporter.export_pdf(self.report_bundle, file_path)
            self._append_log(f"PDF exported: {file_path}")
        except Exception as exc:
            self._append_log(f"PDF export error: {exc}")

    def _process_queue(self):
        try:
            while True:
                event, payload = self.event_queue.get_nowait()
                if event == "log":
                    self._append_log(payload)
                elif event == "progress":
                    self.progress_bar.set(payload)
                elif event == "done":
                    data, bundle = payload
                    self.parsed_data = data
                    self.report_bundle = bundle

                    self.status_label.configure(text="Completed")
                    self._set_running_state(False)
                    self.progress_bar.set(1.0)
                    self._append_log(f"Done. Captured records: {len(data)}")
                    self._render_results(bundle)
                    self._populate_calc_subjects()

                    self.export_excel_btn.configure(state="normal")
                    self.export_pdf_btn.configure(state="normal")

                    if os.environ.get("EMIS_GUI_SMOKE") == "1":
                        self.after(1200, self.destroy)

                elif event == "error":
                    self._set_running_state(False)
                    self.status_label.configure(text="Error")
                    self._append_log("ERROR:")
                    self._append_log(payload)
                    if os.environ.get("EMIS_GUI_SMOKE") == "1":
                        self.after(1200, self.destroy)
        except queue.Empty:
            pass

        self.after(100, self._process_queue)


def run_self_test():
    browser, binary, _ = BrowserResolver.resolve()
    cfg = ParseConfig(
        login=os.environ.get("EMIS_LOGIN", ""),
        password=os.environ.get("EMIS_PASSWORD", ""),
        browser=browser,
        browser_binary=binary,
        user_data_path=EmisParserEngine._default_user_profile_path(browser),
        profile_directory="Default",
        auth_timeout_sec=AUTH_WAIT_TIMEOUT_SEC,
    )

    logger = ConsoleLogger()
    stop_event = threading.Event()
    engine = EmisParserEngine(cfg, logger, stop_event)
    data = engine.parse_grades()
    bundle = ReportBuilder.build_bundle(data)

    excel_path = str((Path.cwd() / "Emis_Report_test").with_suffix(".xlsx"))
    pdf_path = str((Path.cwd() / "Emis_Report_test").with_suffix(".pdf"))
    Exporter.export_excel(bundle, excel_path)
    Exporter.export_pdf(bundle, pdf_path)

    logger.log(f"Self-test completed. Records: {len(data)}")
    logger.log(f"Excel exported: {excel_path}")
    logger.log(f"PDF exported: {pdf_path}")
    logger.log(f"Year total final score: {bundle['year_score']:.2f}")


def main():
    if os.environ.get("EMIS_SELF_TEST") == "1":
        run_self_test()
        return

    app = EmisGuiApp()
    app.mainloop()


if __name__ == "__main__":
    main()
