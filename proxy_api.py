import asyncio
import httpx
import requests
import json
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI(title="Proxy Tool API", description="API for scraping and checking proxies")


PROXY_SOURCES = {
    "http": [
        "https://raw.githubusercontent.com/andigwandi/free-proxy/main/proxy_list.txt",
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/http.txt",
        "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
        "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Https.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/http_proxies.txt",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
        "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/http.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt",
    ],
    "socks4": [
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks4_proxies.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/socks4.txt",
        "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks4/socks4.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks4_proxies.txt",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt",
        "https://raw.githubusercontent.com/Vann-Dev/proxy-list/main/proxies/socks4.txt",
        "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks4.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks4.txt",
    ],
    "socks5": [
        "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt",
        "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        "https://raw.githubusercontent.com/ObcbO/getproxy/master/file/socks5.txt",
        "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt",
        "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
        "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks5_proxies.txt",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
        "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt",
    ],
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

TEST_URLS = [
    "http://httpbin.org/ip",
    "https://api.ipify.org?format=json",
]
GEO_BATCH_URL = "http://ip-api.com/batch"


# ===================== ASYNC SCRAPE =====================
async def async_scrape_url(client, url):
    try:
        resp = await client.get(url)
        if resp.status_code != 200:
            return []
        text = resp.text
        valid = []
        for line in text.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            if ":" in line:
                parts = line.rsplit(":", 1)
                if len(parts) == 2 and parts[1].isdigit():
                    port = int(parts[1])
                    if 1 <= port <= 65535:
                        valid.append(line)
        return valid
    except Exception:
        return []

async def async_scrape_all(sources_dict):
    timeout = httpx.Timeout(15.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=False) as client:
        all_tasks = []
        for ptype, urls in sources_dict.items():
            for url in urls:
                all_tasks.append((ptype, url, async_scrape_url(client, url)))
        results = await asyncio.gather(*[t[2] for t in all_tasks], return_exceptions=True)
        output = {}
        for i, result in enumerate(results):
            ptype = all_tasks[i][0]
            if ptype not in output:
                output[ptype] = set()
            if isinstance(result, list) and result:
                output[ptype].update(result)
        return output

# ===================== GEO =====================
def geo_lookup_batch_sync(ips_list):
    geo = {}
    if not ips_list:
        return geo
    batch_size = 100
    for i in range(0, len(ips_list), batch_size):
        chunk = ips_list[i:i + batch_size]
        payload = [{"query": ip, "fields": "country,countryCode,city,isp"} for ip in chunk]
        try:
            resp = requests.post(GEO_BATCH_URL, json=payload, timeout=15)
            if resp.status_code == 200:
                items = resp.json()
                for idx, item in enumerate(items):
                    ip = chunk[idx] if idx < len(chunk) else ""
                    if item.get("status") == "success":
                        geo[ip] = {"country": item.get("country", "Unknown"), "country_code": item.get("countryCode", "??"), "city": item.get("city", ""), "isp": item.get("isp", "")}
                    else:
                        geo[ip] = {"country": "Unknown", "country_code": "??", "city": "", "isp": ""}
        except Exception:
            for ip in chunk:
                geo[ip] = {"country": "Unknown", "country_code": "??", "city": "", "isp": ""}
        if i + batch_size < len(ips_list):
            time.sleep(1)
    return geo

async def async_geo_lookup_batch(ips_list):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, geo_lookup_batch_sync, ips_list)


# ===================== ASYNC CHECK =====================
async def async_check_proxy(sem, proxy, proxy_type, timeout_val):
    ua = USER_AGENTS[hash(proxy) % len(USER_AGENTS)]
    if proxy_type == "http":
        proxy_url = f"http://{proxy}"
    elif proxy_type == "socks4":
        proxy_url = f"socks4://{proxy}"
    else:
        proxy_url = f"socks5://{proxy}"
    async with sem:
        for test_url in TEST_URLS:
            try:
                start = time.time()
                async with httpx.AsyncClient(
                    proxy=proxy_url,
                    timeout=httpx.Timeout(timeout_val, connect=timeout_val),
                    headers={"User-Agent": ua},
                    follow_redirects=False,
                    verify=False,
                ) as client:
                    resp = await client.get(test_url)
                    if resp.status_code == 200:
                        latency = round((time.time() - start) * 1000)
                        body = resp.text.strip()
                        ip = None
                        try:
                            data = json.loads(body)
                            ip = data.get("origin") or data.get("ip")
                        except Exception:
                            ip = body.split("\n")[0].strip() if body else None
                        return {"proxy": proxy, "type": proxy_type, "ip": ip, "latency": latency, "alive": True}
            except Exception:
                continue
        return {"proxy": proxy, "type": proxy_type, "ip": None, "latency": 0, "alive": False}

async def async_check_all(proxies_dict, selected, concurrency, timeout_val):
    total = sum(len(proxies_dict[t]) for t in selected)
    sem = asyncio.Semaphore(concurrency)
    results = {"http": [], "socks4": [], "socks5": []}
    pending_ips = []
    pending_results = []

    async def flush_geo_batch():
        nonlocal pending_ips, pending_results
        if not pending_ips:
            return
        ips_batch = list(pending_ips)
        res_batch = list(pending_results)
        pending_ips.clear()
        pending_results.clear()
        batch_geo = await async_geo_lookup_batch(ips_batch)
        for ip, result in zip(ips_batch, res_batch):
            if ip in batch_geo:
                g = batch_geo[ip]
                result["country_code"] = g["country_code"]
                result["country"] = g["country"]
            else:
                result["country_code"] = "??"
                result["country"] = "Unknown"

    tasks = []
    for ptype in selected:
        for proxy in proxies_dict.get(ptype, []):
            tasks.append(async_check_proxy(sem, proxy, ptype, timeout_val))

    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result["alive"]:
            results[result["type"]].append(result)
            ip = result.get("ip")
            if ip:
                pending_ips.append(ip)
                pending_results.append(result)
        if len(pending_ips) >= 5:
            await flush_geo_batch()

    await flush_geo_batch()
    return results

# ===================== API ENDPOINTS =====================

class ScrapeRequest(BaseModel):
    types: Optional[List[str]] = ["http", "socks4", "socks5"]

class CheckRequest(BaseModel):
    http: Optional[List[str]] = []
    socks4: Optional[List[str]] = []
    socks5: Optional[List[str]] = []
    concurrency: Optional[int] = 50
    timeout: Optional[float] = 10.0

from fastapi.responses import PlainTextResponse

@app.get("/api/proxies")
async def get_proxies_text(type: Optional[str] = "all"):
    """
    Returns scraped proxies in plain text format (one per line).
    Type can be: http, socks4, socks5, or all
    """
    if type == "all":
        types_to_scrape = ["http", "socks4", "socks5"]
    elif type in ["http", "socks4", "socks5"]:
        types_to_scrape = [type]
    else:
        raise HTTPException(status_code=400, detail="Invalid proxy type. Use http, socks4, socks5, or all.")
        
    sources_to_scrape = {k: PROXY_SOURCES[k] for k in types_to_scrape}
    scraped = await async_scrape_all(sources_to_scrape)
    
    all_proxies = []
    for k, v in scraped.items():
        all_proxies.extend(list(v))
        
    return PlainTextResponse("\n".join(all_proxies))

@app.get("/api/proxies/alive")
async def get_alive_proxies_text(type: Optional[str] = "all", timeout: float = 5.0, concurrency: int = 150):
    """
    Scrapes and checks proxies, returning ONLY the alive ones in plain text.
    """
    if type == "all":
        types_to_scrape = ["http", "socks4", "socks5"]
    elif type in ["http", "socks4", "socks5"]:
        types_to_scrape = [type]
    else:
        raise HTTPException(status_code=400, detail="Invalid proxy type.")
        
    # 1. Scrape
    sources_to_scrape = {k: PROXY_SOURCES[k] for k in types_to_scrape}
    scraped = await async_scrape_all(sources_to_scrape)
    
    # 2. Check
    proxies_dict = {k: list(v) for k, v in scraped.items()}
    selected = list(proxies_dict.keys())
    
    # Run the check
    results = await async_check_all(proxies_dict, selected, concurrency, timeout)
    
    # 3. Extract alive proxies
    alive_proxies = []
    for k, v_list in results.items():
        for p in v_list:
            if p.get("alive"):
                alive_proxies.append(p["proxy"])
                
    return PlainTextResponse("\n".join(alive_proxies))


@app.get("/")
async def root():
    return {"message": "Proxy Tool API is running. Check /docs for API documentation."}

@app.post("/api/scrape")
async def scrape_proxies(req: ScrapeRequest):
    """
    Scrape proxies from defined sources.
    Pass types list: ["http", "socks4", "socks5"]
    """
    sources_to_scrape = {k: PROXY_SOURCES[k] for k in req.types if k in PROXY_SOURCES}
    if not sources_to_scrape:
        raise HTTPException(status_code=400, detail="Invalid proxy types specified.")
    
    scraped = await async_scrape_all(sources_to_scrape)
    # Convert sets to list
    return {k: list(v) for k, v in scraped.items()}

@app.post("/api/check")
async def check_proxies(req: CheckRequest):
    """
    Check proxies and get their geo location and latency.
    """
    proxies_dict = {}
    selected = []
    if req.http:
        proxies_dict["http"] = req.http
        selected.append("http")
    if req.socks4:
        proxies_dict["socks4"] = req.socks4
        selected.append("socks4")
    if req.socks5:
        proxies_dict["socks5"] = req.socks5
        selected.append("socks5")
    
    if not selected:
        raise HTTPException(status_code=400, detail="No proxies provided to check.")
    
    results = await async_check_all(proxies_dict, selected, req.concurrency, req.timeout)
    return results

@app.post("/api/auto")
async def auto_scrape_and_check(req: ScrapeRequest, concurrency: int = 50, timeout: float = 10.0):
    """
    Automatically scrape proxies based on types and then check them all, returning only the alive ones.
    """
    sources_to_scrape = {k: PROXY_SOURCES[k] for k in req.types if k in PROXY_SOURCES}
    if not sources_to_scrape:
        raise HTTPException(status_code=400, detail="Invalid proxy types specified.")
    
    # 1. Scrape
    scraped = await async_scrape_all(sources_to_scrape)
    
    # 2. Check
    proxies_dict = {k: list(v) for k, v in scraped.items()}
    selected = list(proxies_dict.keys())
    
    results = await async_check_all(proxies_dict, selected, concurrency, timeout)
    return results
