#!/bin/bash

# Import Fix Script for modb-iouring
# 修复modb-iouring模块中的剩余导入问题

set -e

echo "修复modb-iouring模块的导入依赖..."

TARGET_DIR="modb-iouring/src"

# 修复util包内的互相引用
echo "修复util包内的导入..."
find "${TARGET_DIR}" -name "*.java" -type f -exec sed -i 's/import dk\.ku\.di\.dms\.vms\.web_common\.iouring\.util/import dk.ku.di.dms.vms.modb.iouring.util/g' {} \;

# 修复c包内的引用
echo "修复c目录相关的引用..."
find "${TARGET_DIR}" -name "*.java" -type f -exec sed -i 's/import dk\.ku\.di\.dms\.vms\.web_common\.iouring\.c/import dk.ku.di.dms.vms.modb.iouring.c/g' {} \;

# 修复测试文件中对util包的引用
echo "修复测试文件中的导入..."
find "${TARGET_DIR}/test" -name "*.java" -type f -exec sed -i 's/import dk\.ku\.di\.dms\.vms\.web_common\.iouring\.util/import dk.ku.di.dms.vms.modb.iouring.util/g' {} \; 2>/dev/null || true

# 更新c头文件的引用（如果有的话）
echo "检查和更新c头文件引用..."
if [ -d "${TARGET_DIR}/main/java/dk/ku/di/dms/vms/modb/iouring/c" ]; then
    find "${TARGET_DIR}/main/java/dk/ku/di/dms/vms/modb/iouring/c" -name "*.h" -type f -exec sed -i 's/web_common/modb/g' {} \; 2>/dev/null || true
    find "${TARGET_DIR}/main/java/dk/ku/di/dms/vms/modb/iouring/c" -name "*.c" -type f -exec sed -i 's/web_common/modb/g' {} \; 2>/dev/null || true
fi

echo "✓ 导入修复完成！"

# 检查是否还有遗留的web_common引用
echo ""
echo "检查剩余的web_common引用..."
REMAINING=$(grep -r "web_common" "${TARGET_DIR}" 2>/dev/null || true)
if [ -n "${REMAINING}" ]; then
    echo "发现剩余的web_common引用:"
    echo "${REMAINING}"
    echo "⚠ 需要手动检查和修复"
else
    echo "✓ 没有发现剩余的web_common引用"
fi

echo ""
echo "导入修复脚本执行完成！" 