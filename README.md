# Sentinel-1-s-POEORB
## 工作流程
读取 config.ini 中的账号、密码、SLC 路径、轨道保存路径和并行数 → 递归扫描 SLC 目录(zip 和 SAFE 都能识别),从文件名解析出卫星号(S1A/S1B/S1C)和成像日期 → 每个日期展开为前一天、当天、后一天共三天 → 从 ASF (s1qc.asf.alaska.edu/aux_poeorb) 获取轨道列表,匹配完整覆盖每一天的 POEORB 文件(同一天有多版本时取生成时间最新的)→ 多线程并行下载。

## 几个细节
已存在的文件自动跳过,可以重复运行补漏;下载先写 .tmp 再改名,不会留下半截文件;失败自动重试(次数可配);密码错误会直接提示 401;如果某天的精密轨道还没发布(POEORB 大约延迟 20 天),会列出警告而不是报错中断。

## 使用方法
填好 config.ini 里的 Earthdata 账号(和 ASF Vertex 登录是同一个账号)及路径,然后 python s1_orbit_download.py 即可,依赖只有 requests(pip install requests)。

## 说明
实际上每个 POEORB 文件本身的有效期就覆盖约 26 小时(前一天约 23 点到后一天约 1 点),按三天各下一个文件,GAMMA 的 S1_OPOD_vec 等工具在处理跨天数据时都能找到轨道,冗余但最保险。
