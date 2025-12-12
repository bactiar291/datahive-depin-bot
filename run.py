import requests, threading, time, random, uuid, argparse, csv, sys
from datetime import datetime
from colorama import init, Fore, Style
from urllib.parse import urlparse

init(autoreset=True)

API_BASE = "https://api.datahive.ai/api"
DEFAULT_PING_INTERVAL = 60.0
INFO_CHECK_INTERVAL = 300.0
JITTER_MAX = 2.0
MAX_BACKOFF = 60.0
INITIAL_BACKOFF = 5.0

def now(): 
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def mask(tok, keep=8):
    if not tok: return "N/A"
    s = tok.strip()
    return "..." + s[-keep:] if len(s) > keep else s

def normalize_token(line):
    s = line.strip()
    if not s: return None
    if s.lower().startswith("bearer "):
        s = s.split(" ",1)[1].strip()
    return s

def parse_proxy(proxy_str):
    if not proxy_str or proxy_str.strip() == "":
        return None
    
    proxy_str = proxy_str.strip()
    parts = proxy_str.split()
    
    try:
        if proxy_str.startswith("socks5://") or proxy_str.startswith("http://") or proxy_str.startswith("https://"):
            return {'http': proxy_str, 'https': proxy_str}
        
        if len(parts) == 4:
            ip, port, user, pwd = parts
            proxy_url = f"http://{user}:{pwd}@{ip}:{port}"
            return {'http': proxy_url, 'https': proxy_url}
        
        elif len(parts) == 2:
            ip, port = parts
            proxy_url = f"http://{ip}:{port}"
            return {'http': proxy_url, 'https': proxy_url}
        
        elif ":" in proxy_str and "@" in proxy_str:
            if not proxy_str.startswith("http"):
                proxy_str = f"http://{proxy_str}"
            return {'http': proxy_str, 'https': proxy_str}
        
        elif ":" in proxy_str:
            proxy_url = f"http://{proxy_str}"
            return {'http': proxy_url, 'https': proxy_url}
        
        return None
    except Exception:
        return None

def load_tokens(path):
    tokens = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                tok = normalize_token(line)
                if tok:
                    tokens.append(tok)
    except FileNotFoundError:
        raise
    return tokens

def load_proxies(path):
    proxies = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                proxy = parse_proxy(line)
                if proxy:
                    proxies.append(proxy)
    except FileNotFoundError:
        return []
    return proxies

def fetch_configuration(token, proxy=None):
    headers = {
        "Accept": "application/json",
        "User-Agent": "DataHiveBot/1.0",
        "Authorization": f"Bearer {token}"
    }
    try:
        r = requests.get(
            f"{API_BASE}/configuration", 
            headers=headers, 
            proxies=proxy,
            timeout=10
        )
        if r.status_code in (200,201):
            try:
                return r.json()
            except Exception:
                return None
        return None
    except Exception:
        return None

class AccountWorker(threading.Thread):
    def __init__(self, token, proxy=None, name=None, global_config=None, 
                 ping_override=None, logfile=None, debug=False):
        super().__init__(daemon=True)
        self.token = token.strip()
        self.proxy = proxy
        self.name = name or f"Akun-{self.token[-6:]}"
        self.session = requests.Session()
        self.device_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, self.token))
        self.stats = {
            "total": 0, 
            "ok": 0, 
            "fail": 0, 
            "points": 0, 
            "start": time.time(),
            "consecutive_fails": 0,
            "status": "initializing"
        }
        self.running = True
        self.global_config = global_config or {}
        self.ping_override = ping_override
        self.debug = debug
        self.logfile = logfile
        self._prepare_headers()
        
        if self.proxy:
            self.session.proxies.update(self.proxy)
            proxy_display = list(self.proxy.values())[0] if self.proxy else "none"
            if '@' in proxy_display:
                proxy_display = proxy_display.split('@')[1]
            if self.debug:
                print(Fore.CYAN + f"[{now()}] [{self.name}] Using proxy: {proxy_display}" + Style.RESET_ALL)

    def _prep_val(self, k, fallback):
        return self.global_config.get(k) or self.global_config.get(k.replace("_","")) or fallback

    def _prepare_headers(self):
        os_type = random.choice(['windows', 'linux', 'macos'])
        chrome_ver = random.randint(120, 143)
        
        if os_type == 'windows':
            win_ver = random.choice(['10.0.0', '11.0.0', '10.0.19044', '11.0.22000'])
            ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver}.0.0.0 Safari/537.36"
            device_os = f"Windows {win_ver}"
            device_name = random.choice(['windows pc', 'desktop', 'workstation', 'pc'])
            cpu_arch = "x86_64"
        elif os_type == 'linux':
            ua = f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver}.0.0.0 Safari/537.36"
            device_os = random.choice(['Ubuntu 22.04', 'Ubuntu 20.04', 'Debian 11', 'Fedora 38', 'Linux'])
            device_name = random.choice(['linux pc', 'desktop', 'workstation'])
            cpu_arch = "x86_64"
        else:
            mac_ver = random.choice(['10_15_7', '11_6_0', '12_5_0', '13_0_0', '14_1_0'])
            safari_ver = random.choice(['605.1.15', '604.1.38', '616.1.27'])
            ua = f"Mozilla/5.0 (Macintosh; Intel Mac OS X {mac_ver}) AppleWebKit/{safari_ver} Version/17.1 Safari/{safari_ver}"
            device_os = f"macOS {mac_ver.replace('_', '.')}"
            device_name = random.choice(['macbook', 'imac', 'mac', 'macbook pro'])
            cpu_arch = random.choice(['x86_64', 'arm64'])
        
        app_ver = self._prep_val("min_extension_version", "0.2.5")
        device_model = f"PC {cpu_arch} - Chrome {chrome_ver}"
        
        cpu_models = [
            "Intel(R) Core(TM) i5-6200U CPU @ 2.30GHz",
            "Intel(R) Core(TM) i7-8550U CPU @ 1.80GHz",
            "Intel(R) Core(TM) i5-10210U CPU @ 1.60GHz",
            "Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz",
            "Intel(R) Core(TM) i5-1135G7 CPU @ 2.40GHz",
            "Intel(R) Core(TM) i7-1165G7 CPU @ 2.80GHz",
            "AMD Ryzen 5 3600 6-Core Processor",
            "AMD Ryzen 7 5800X 8-Core Processor",
            "AMD Ryzen 5 5600X 6-Core Processor",
            "Intel(R) Core(TM) i9-10900K CPU @ 3.70GHz",
            "Intel(R) Core(TM) i5-11400F CPU @ 2.60GHz",
            "AMD Ryzen 9 5900X 12-Core Processor"
        ]
        
        cpu_model = random.choice(cpu_models)
        if cpu_arch == 'arm64':
            cpu_model = "Apple M1" if random.random() > 0.5 else "Apple M2"
        
        cpu_count = str(random.choice([2, 4, 6, 8, 12, 16]))

        headers = {
            "User-Agent": ua,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
            "x-app-version": str(app_ver),
            "x-platform": "chrome",
            "x-extension-version": str(app_ver),
            "x-browser": "chrome",
            "x-device-id": self.device_id,
            "x-device-model": device_model,
            "x-device-name": device_name,
            "x-device-os": device_os,
            "x-device-type": "extension",
            "x-user-language": "en-US",
            "x-user-agent": ua,
            "x-cpu-model": cpu_model,
            "x-cpu-architecture": cpu_arch,
            "x-cpu-processor-count": cpu_count,
            "x-s": "f",
        }
        self.session.headers.update(headers)
        if self.debug:
            print(Fore.MAGENTA + f"[{now()}] [{self.name}] Headers prepared" + Style.RESET_ALL)

    def dbg(self, *a):
        if self.debug:
            print(Fore.MAGENTA + "[" + now() + "] [" + self.name + "] " + " ".join(str(x) for x in a) + Style.RESET_ALL)

    def get_user(self):
        try:
            r = self.session.get(f"{API_BASE}/user", timeout=10)
            self.dbg("GET /user ->", r.status_code)
            if r.status_code == 200:
                data = r.json()
                self.stats["points"] = data.get("points", 0)
                self.stats["status"] = "active"
                return data
            elif r.status_code in [401, 403]:
                self.stats["status"] = "invalid_token"
            return None
        except requests.exceptions.ProxyError as e:
            self.dbg("PROXY ERROR get_user:", str(e)[:100])
            self.stats["status"] = "proxy_error"
            return None
        except requests.exceptions.Timeout:
            self.dbg("TIMEOUT get_user")
            self.stats["status"] = "timeout"
            return None
        except Exception as e:
            self.dbg("ERR get_user:", str(e)[:100])
            self.stats["status"] = "error"
            return None

    def ping(self):
        try:
            r = self.session.post(f"{API_BASE}/ping", timeout=10)
            self.dbg("POST /ping ->", r.status_code)
            self.stats["total"] += 1
            
            if r.status_code == 200:
                self.stats["ok"] += 1
                self.stats["consecutive_fails"] = 0
                self.stats["status"] = "active"
                return True, r
            elif r.status_code in [401, 403]:
                self.stats["fail"] += 1
                self.stats["consecutive_fails"] += 1
                self.stats["status"] = "invalid_token"
                return False, r
            else:
                self.stats["fail"] += 1
                self.stats["consecutive_fails"] += 1
                return False, r
                
        except requests.exceptions.ProxyError as e:
            self.stats["total"] += 1
            self.stats["fail"] += 1
            self.stats["consecutive_fails"] += 1
            self.stats["status"] = "proxy_error"
            self.dbg("PROXY ERROR ping:", str(e)[:100])
            return False, None
        except requests.exceptions.Timeout:
            self.stats["total"] += 1
            self.stats["fail"] += 1
            self.stats["consecutive_fails"] += 1
            self.stats["status"] = "timeout"
            self.dbg("TIMEOUT ping")
            return False, None
        except Exception as e:
            self.stats["total"] += 1
            self.stats["fail"] += 1
            self.stats["consecutive_fails"] += 1
            self.stats["status"] = "error"
            self.dbg("ERR ping:", str(e)[:100])
            return False, None

    def _log_csv(self, row):
        if not self.logfile: return
        try:
            with open(self.logfile, "a", newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(row)
        except Exception as e:
            self.dbg("ERR write log:", e)

    def run(self):
        user = self.get_user()
        if user:
            print(Fore.GREEN + f"[{now()}] [{self.name}] ✓ Login OK | email: {user.get('email','N/A')} | points: {self.stats['points']}" + Style.RESET_ALL)
            if self.logfile:
                self._log_csv(["timestamp","account","action","status","http_code","points","message"])
        else:
            status_msg = self.stats["status"]
            print(Fore.YELLOW + f"[{now()}] [{self.name}] ⚠ Login failed ({status_msg}) | token: {mask(self.token)}" + Style.RESET_ALL)
            if status_msg == "invalid_token":
                print(Fore.RED + f"[{now()}] [{self.name}] ✗ Token invalid, stopping worker" + Style.RESET_ALL)
                self.running = False
                return

        interval = DEFAULT_PING_INTERVAL
        if self.ping_override:
            try:
                interval = float(self.ping_override)
            except:
                pass
        else:
            cfg_delay = self.global_config.get("job_execution_delay") or self.global_config.get("jobExecutionDelay")
            try:
                if cfg_delay is not None:
                    interval = float(cfg_delay)
            except:
                pass

        last_info = time.time()
        backoff = INITIAL_BACKOFF

        while self.running:
            ok, resp = self.ping()
            
            if ok:
                pts = self.stats["points"]
                snippet = ""
                try:
                    js = resp.json()
                    snippet = str(js)[:200]
                except Exception:
                    snippet = resp.text[:200] if resp is not None else ""
                
                print(Fore.GREEN + f"[{now()}] [{self.name}] ✓ PING OK | #{self.stats['total']} pts:{pts}" + Style.RESET_ALL)
                self._log_csv([now(), self.name, "ping", "ok", resp.status_code if resp else "", pts, snippet])
                backoff = INITIAL_BACKOFF
                
            else:
                status = self.stats["status"]
                print(Fore.RED + f"[{now()}] [{self.name}] ✗ PING FAIL ({status}) | #{self.stats['total']} fails:{self.stats['consecutive_fails']}" + Style.RESET_ALL)
                
                if resp is not None:
                    txt = ""
                    try:
                        txt = resp.text[:300]
                    except:
                        txt = ""
                    if txt:
                        print(Fore.YELLOW + f"  → HTTP {resp.status_code}: {txt}" + Style.RESET_ALL)
                    self._log_csv([now(), self.name, "ping", "fail", resp.status_code, self.stats["points"], txt[:200]])
                else:
                    self._log_csv([now(), self.name, "ping", "fail", "", self.stats["points"], status])
                
                if status == "invalid_token":
                    print(Fore.RED + f"[{now()}] [{self.name}] ✗ Token invalid, stopping" + Style.RESET_ALL)
                    self.running = False
                    break
                
                sleep_time = min(backoff, MAX_BACKOFF)
                print(Fore.YELLOW + f"[{now()}] [{self.name}] ⏸ Backoff {sleep_time:.1f}s" + Style.RESET_ALL)
                time.sleep(sleep_time)
                backoff = min(backoff * 1.5, MAX_BACKOFF)
                continue

            if time.time() - last_info >= INFO_CHECK_INTERVAL:
                u = self.get_user()
                if u:
                    print(Fore.CYAN + f"[{now()}] [{self.name}] ℹ Points: {self.stats['points']}" + Style.RESET_ALL)
                    self._log_csv([now(), self.name, "info", "ok", 200, self.stats["points"], ""])
                last_info = time.time()

            jitter = random.uniform(-JITTER_MAX, JITTER_MAX)
            time.sleep(max(0.5, interval + jitter))

    def stop(self):
        self.running = False

def main():
    ap = argparse.ArgumentParser(description="DataHive multi-account bot dengan proxy support")
    ap.add_argument("-a","--accounts-file", default="akun.txt", help="File akun (satu token per baris)")
    ap.add_argument("-x","--proxy-file", default="proxy.txt", help="File proxy (opsional)")
    ap.add_argument("-p","--ping-interval", default=None, help="Override ping interval (detik)")
    ap.add_argument("-l","--logfile", default=None, help="CSV log file")
    ap.add_argument("-d","--debug", action="store_true", help="Debug mode")
    args = ap.parse_args()

    print(Fore.CYAN + """
╔══════════════════════════════════════╗
║     DataHive Multi-Account Bot       ║
║        Proxy Support Enabled         ║
║             Bactiar291               ║
╚══════════════════════════════════════╝
    """ + Style.RESET_ALL)

    try:
        tokens = load_tokens(args.accounts_file)
    except FileNotFoundError:
        print(Fore.RED + f"[{now()}] ✗ File {args.accounts_file} tidak ditemukan" + Style.RESET_ALL)
        print(Fore.YELLOW + "Buat file akun.txt dengan format satu token per baris" + Style.RESET_ALL)
        sys.exit(1)

    if not tokens:
        print(Fore.RED + f"[{now()}] ✗ Tidak ada token valid di {args.accounts_file}" + Style.RESET_ALL)
        sys.exit(1)

    proxies = load_proxies(args.proxy_file)
    if proxies:
        print(Fore.GREEN + f"[{now()}] ✓ Loaded {len(proxies)} proxy(s)" + Style.RESET_ALL)
    else:
        print(Fore.YELLOW + f"[{now()}] ⚠ No proxy file or empty, running without proxy" + Style.RESET_ALL)

    print(Fore.GREEN + f"[{now()}] ✓ Loaded {len(tokens)} token(s)" + Style.RESET_ALL)

    global_config = fetch_configuration(tokens[0], proxies[0] if proxies else None)
    if global_config:
        delay = global_config.get('job_execution_delay') or global_config.get('jobExecutionDelay')
        print(Fore.CYAN + f"[{now()}] ✓ Config loaded (ping interval: {delay}s)" + Style.RESET_ALL)
    else:
        print(Fore.YELLOW + f"[{now()}] ⚠ Using default ping interval: {DEFAULT_PING_INTERVAL}s" + Style.RESET_ALL)

    workers = []
    for i, token in enumerate(tokens):
        name = f"Akun-{i+1}"
        proxy = proxies[i % len(proxies)] if proxies else None
        
        w = AccountWorker(
            token=token,
            proxy=proxy,
            name=name,
            global_config=global_config or {},
            ping_override=args.ping_interval,
            logfile=args.logfile,
            debug=args.debug
        )
        w.start()
        workers.append(w)
        time.sleep(0.3)

    print(Fore.GREEN + f"\n[{now()}] ✓ Started {len(workers)} worker(s)" + Style.RESET_ALL)
    print(Fore.CYAN + "Press Ctrl+C to stop\n" + Style.RESET_ALL)

    try:
        while True:
            time.sleep(60)
            active = sum(1 for w in workers if w.running)
            print(Fore.MAGENTA + f"\n[{now()}] ═══ SUMMARY ({active}/{len(workers)} active) ═══" + Style.RESET_ALL)
            for w in workers:
                if not w.running:
                    status_icon = "✗"
                    color = Fore.RED
                elif w.stats["status"] == "active":
                    status_icon = "✓"
                    color = Fore.GREEN
                else:
                    status_icon = "⚠"
                    color = Fore.YELLOW
                
                up = int(time.time() - w.stats["start"])
                m = up // 60
                success_rate = (w.stats['ok'] / w.stats['total'] * 100) if w.stats['total'] > 0 else 0
                
                print(color + f"  {status_icon} [{w.name}] {w.stats['status']} | "
                      f"up:{m}m | ping:{w.stats['total']} (✓{w.stats['ok']}/✗{w.stats['fail']}) | "
                      f"rate:{success_rate:.1f}% | pts:{w.stats['points']}" + Style.RESET_ALL)
                
    except KeyboardInterrupt:
        print(Fore.YELLOW + "\n\n⏸ Stopping workers..." + Style.RESET_ALL)
        for w in workers:
            w.stop()
        time.sleep(1)
        print(Fore.GREEN + "✓ Stopped gracefully\n" + Style.RESET_ALL)

if __name__ == "__main__":
    main()
