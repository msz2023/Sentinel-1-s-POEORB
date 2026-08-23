#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sentinel-1 精密轨道数据 (AUX_POEORB) 自动下载脚本
==================================================
功能:
  1. 从配置文件读取 ASF(Earthdata) 账号、密码、SLC 路径、轨道保存路径、并行下载数
  2. 自动扫描 SLC 目录(支持 .zip / .SAFE),从文件名解析卫星号(S1A/S1B/S1C)和成像日期
  3. 对每个成像日期,下载覆盖【前一天、当天、后一天】共三天的精密轨道文件
  4. 多线程并行下载,支持断点跳过(已存在且完整的文件不重复下载)、失败重试

用法:
  python s1_orbit_download.py                # 使用脚本同目录下的 config.ini
  python s1_orbit_download.py -c my.ini      # 指定配置文件

依赖:
  pip install requests
"""

import os
import re
import sys
import argparse
import configparser
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    sys.exit("缺少 requests 库,请先执行: pip install requests")

# ASF 精密轨道数据列表页(需要 Earthdata 账号认证下载)
POEORB_BASE_URL = "https://s1qc.asf.alaska.edu/aux_poeorb/"

# POEORB 文件名格式:
# S1A_OPER_AUX_POEORB_OPOD_20200122T120706_V20200101T225942_20200103T005942.EOF
EOF_PATTERN = re.compile(
    r"(S1[ABC])_OPER_AUX_POEORB_OPOD_(\d{8}T\d{6})_V(\d{8}T\d{6})_(\d{8}T\d{6})\.EOF"
)

# SLC 文件名格式(zip、SAFE 目录、或解压后的文件夹名均可匹配):
# S1A_IW_SLC__1SDV_20200102T103829_20200102T103856_030628_038242_31A1.zip
SLC_PATTERN = re.compile(
    r"(S1[ABC])_\w{2}_SLC__\w{4}_(\d{8})T\d{6}_\d{8}T\d{6}_\w+"
)


# ---------------------------------------------------------------------------
# Earthdata 登录会话:重定向到 urs.earthdata.nasa.gov 时保留认证头
# ---------------------------------------------------------------------------
class EarthdataSession(requests.Session):
    AUTH_HOST = "urs.earthdata.nasa.gov"

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url
        if "Authorization" in headers:
            original = urlparse(response.request.url).hostname
            redirect = urlparse(url).hostname
            if (original != redirect
                    and redirect != self.AUTH_HOST
                    and original != self.AUTH_HOST):
                del headers["Authorization"]
        return prepared_request


# ---------------------------------------------------------------------------
# 读取配置
# ---------------------------------------------------------------------------
def load_config(cfg_path):
    if not os.path.isfile(cfg_path):
        sys.exit(f"[错误] 配置文件不存在: {cfg_path}")

    cfg = configparser.ConfigParser()
    cfg.read(cfg_path, encoding="utf-8")

    try:
        conf = {
            "username":  cfg.get("account", "username").strip(),
            "password":  cfg.get("account", "password").strip(),
            "slc_dir":   cfg.get("paths", "slc_dir").strip(),
            "orbit_dir": cfg.get("paths", "orbit_dir").strip(),
            "parallel":  cfg.getint("download", "parallel", fallback=4),
            "retry":     cfg.getint("download", "retry", fallback=3),
            "timeout":   cfg.getint("download", "timeout", fallback=120),
        }
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        sys.exit(f"[错误] 配置文件缺少必要项: {e}")

    if not os.path.isdir(conf["slc_dir"]):
        sys.exit(f"[错误] SLC 目录不存在: {conf['slc_dir']}")
    os.makedirs(conf["orbit_dir"], exist_ok=True)
    conf["parallel"] = max(1, conf["parallel"])
    return conf


# ---------------------------------------------------------------------------
# 扫描 SLC 目录,解析出 (卫星号, 成像日期) 集合
# ---------------------------------------------------------------------------
def scan_slc_dates(slc_dir):
    found = set()
    for root, dirs, files in os.walk(slc_dir):
        for name in list(dirs) + list(files):
            m = SLC_PATTERN.search(name)
            if m:
                sat = m.group(1)
                day = datetime.strptime(m.group(2), "%Y%m%d").date()
                found.add((sat, day))
    return sorted(found)


# ---------------------------------------------------------------------------
# 获取 ASF 上全部 POEORB 文件列表,建立索引
# 返回: {(sat, 覆盖日期): 文件名}   同一天取生成时间最新的版本
# ---------------------------------------------------------------------------
def fetch_orbit_index(session, timeout):
    print(f"[信息] 正在获取 ASF 轨道文件列表: {POEORB_BASE_URL} (文件较多,请稍候...)")
    r = session.get(POEORB_BASE_URL, timeout=timeout)
    r.raise_for_status()

    index = {}   # key: (sat, date) -> (production_time, filename)
    for m in EOF_PATTERN.finditer(r.text):
        fname = m.group(0)
        sat = m.group(1)
        prod = m.group(2)
        vstart = datetime.strptime(m.group(3), "%Y%m%dT%H%M%S")
        vstop = datetime.strptime(m.group(4), "%Y%m%dT%H%M%S")

        # 该轨道文件完整覆盖的日期(通常验证期约26小时,完整覆盖中间那一天)
        day = vstart.date()
        while day <= vstop.date():
            day_start = datetime(day.year, day.month, day.day, 0, 0, 0)
            day_end = datetime(day.year, day.month, day.day, 23, 59, 59)
            if vstart <= day_start and vstop >= day_end:
                key = (sat, day)
                if key not in index or prod > index[key][0]:
                    index[key] = (prod, fname)
            day += timedelta(days=1)

    print(f"[信息] 列表解析完成,共索引 {len(index)} 条 (卫星,日期) 记录")
    return {k: v[1] for k, v in index.items()}


# ---------------------------------------------------------------------------
# 下载单个轨道文件(带重试、断点跳过、临时文件防止半截文件)
# ---------------------------------------------------------------------------
def download_one(fname, orbit_dir, username, password, retry, timeout):
    url = POEORB_BASE_URL + fname
    dst = os.path.join(orbit_dir, fname)
    tmp = dst + ".tmp"

    if os.path.isfile(dst) and os.path.getsize(dst) > 0:
        return fname, "跳过(已存在)"

    last_err = None
    for attempt in range(1, retry + 1):
        try:
            session = EarthdataSession(username, password)
            with session.get(url, stream=True, timeout=timeout) as r:
                if r.status_code == 401:
                    return fname, "失败: 账号或密码错误(401)"
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
            if os.path.getsize(tmp) == 0:
                raise IOError("下载得到空文件")
            os.replace(tmp, dst)
            return fname, "成功"
        except Exception as e:
            last_err = e
            if os.path.isfile(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
    return fname, f"失败: {last_err} (已重试{retry}次)"


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Sentinel-1 精密轨道数据自动下载")
    default_cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
    parser.add_argument("-c", "--config", default=default_cfg,
                        help="配置文件路径 (默认: 脚本同目录 config.ini)")
    args = parser.parse_args()

    conf = load_config(args.config)

    # 1. 扫描 SLC,得到成像日期
    slc_dates = scan_slc_dates(conf["slc_dir"])
    if not slc_dates:
        sys.exit(f"[错误] 在 {conf['slc_dir']} 中未找到任何 Sentinel-1 SLC 文件")
    print(f"[信息] 共发现 {len(slc_dates)} 个 SLC 成像日期:")
    for sat, day in slc_dates:
        print(f"        {sat}  {day}")

    # 2. 生成需要的轨道日期(前一天、当天、后一天)
    need_days = set()
    for sat, day in slc_dates:
        for off in (-1, 0, 1):
            need_days.add((sat, day + timedelta(days=off)))
    need_days = sorted(need_days)
    print(f"[信息] 按前/当/后三天展开后,共需 {len(need_days)} 个 (卫星,日期) 的轨道数据")

    # 3. 获取 ASF 轨道列表并匹配
    session = EarthdataSession(conf["username"], conf["password"])
    try:
        index = fetch_orbit_index(session, conf["timeout"])
    except Exception as e:
        sys.exit(f"[错误] 获取轨道列表失败: {e}")

    tasks, missing = [], []
    for sat, day in need_days:
        fname = index.get((sat, day))
        if fname:
            tasks.append(fname)
        else:
            missing.append((sat, day))
    tasks = sorted(set(tasks))

    if missing:
        print("[警告] 以下日期未在 ASF 找到对应精密轨道(可能太新,POEORB约延迟20天发布):")
        for sat, day in missing:
            print(f"        {sat}  {day}")

    if not tasks:
        sys.exit("[错误] 没有可下载的轨道文件")

    print(f"[信息] 需下载 {len(tasks)} 个轨道文件,并行数 = {conf['parallel']}")
    print(f"[信息] 保存目录: {conf['orbit_dir']}\n")

    # 4. 并行下载
    ok = skip = fail = 0
    with ThreadPoolExecutor(max_workers=conf["parallel"]) as pool:
        futures = {
            pool.submit(download_one, f, conf["orbit_dir"],
                        conf["username"], conf["password"],
                        conf["retry"], conf["timeout"]): f
            for f in tasks
        }
        done = 0
        for fut in as_completed(futures):
            fname, status = fut.result()
            done += 1
            print(f"  [{done}/{len(tasks)}] {fname}  ->  {status}")
            if status == "成功":
                ok += 1
            elif status.startswith("跳过"):
                skip += 1
            else:
                fail += 1

    # 5. 汇总
    print("\n========== 下载汇总 ==========")
    print(f"  成功: {ok}    跳过(已存在): {skip}    失败: {fail}")
    if missing:
        print(f"  未找到轨道的日期: {len(missing)} 个(见上方警告)")
    print("==============================")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
