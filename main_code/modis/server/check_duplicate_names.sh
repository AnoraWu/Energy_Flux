#!/bin/bash

# 用法检查
if [ -z "$1" ]; then
    echo "Usage: bash check_duplicate_names.sh <target_directory>"
    exit 1
fi

ROOT_DIR="$1"

echo "Checking duplicate filenames inside each subfolder of:"
echo "$ROOT_DIR"
echo "----------------------------------------"

# 遍历每一个子目录
find "$ROOT_DIR" -type d | while read -r dir; do
    # 获取该目录下所有普通文件名（不含路径）
    duplicates=$(ls -1 "$dir" 2>/dev/null | sort | uniq -d)

    if [ -n "$duplicates" ]; then
        echo "⚠️  Duplicate files found in:"
        echo "Directory: $dir"
        echo "$duplicates"
        echo ""

        # 打印这些重复文件的完整路径
        for f in $duplicates; do
            find "$dir" -maxdepth 1 -name "$f"
        done

        echo "----------------------------------------"
    fi
done

echo "✅ Duplicate filename check finished."