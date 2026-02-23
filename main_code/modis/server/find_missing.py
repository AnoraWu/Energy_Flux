import os
from pathlib import Path

# 你的 rawdata 根目录
ROOT = Path("/project/mgreenst/cloudseeding/rawdata/MOD06_L2/2020")

# 你上传的文件列表
LIST_FILE = "/mnt/data/check_download_2020.out"

missing = []

with open(LIST_FILE, "r") as f:
    for line in f:
        name = line.strip()
        if not name.endswith(".hdf"):
            continue

        # 解析 day — 文件名格式： MOD06_L2.AYYYYDDD.HHMM.061.XXXXXXXXXXXX.hdf
        day = name[14:17]    # A2020DDD → DDD

        # 该文件应当在的目录
        dest = ROOT / day / name

        if not dest.exists():
            missing.append((day, name))

# 输出缺失文件列表
with open("missing_2020.txt", "w") as out:
    for day, name in missing:
        out.write(f"{day} {name}\n")

print(f"Found {len(missing)} missing files.")