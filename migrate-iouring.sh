#!/bin/bash

# IoUring Migration Script
# 将web_common中的iouring文件迁移到正确的模块中

set -e  # 遇到错误就退出

echo "开始迁移IoUring文件到正确的模块..."

# 定义源路径和目标路径
SOURCE_MAIN="web_common/src/main/java/dk/ku/di/dms/vms/web_common/iouring"
SOURCE_TEST="web_common/src/test/java/dk/ku/di/dms/vms/web_common/iouring"

# modb-iouring: 纯粹的io_uring底层实现
IOURING_PACKAGE_PATH="dk/ku/di/dms/vms/modb/iouring"
IOURING_MAIN="modb-iouring/src/main/java/${IOURING_PACKAGE_PATH}"
IOURING_TEST="modb-iouring/src/test/java/${IOURING_PACKAGE_PATH}"

# modb-common: logging相关的实现
COMMON_PACKAGE_PATH="dk/ku/di/dms/vms/modb/common"
COMMON_MAIN="modb-common/src/main/java/${COMMON_PACKAGE_PATH}"

# 创建目标目录结构
echo "创建目标目录结构..."
mkdir -p "${IOURING_MAIN}"
mkdir -p "${IOURING_TEST}"
mkdir -p "${COMMON_MAIN}/logging"

# 定义需要放到各个模块的文件
IOURING_FILES=(
    "IoUring.java"
    "IoUringFile.java"
    "AbstractIoUringChannel.java"
    "AbstractIoUringSocket.java"
    "IoUringChannelGroup.java"
    "IoUringServerSocket.java"
    "IoUringServerSocketChannel.java"
    "IoUringSocket.java"
    "IoUringSocketChannel.java"
)

LOGGING_FILES=(
    "IoUringLoggingHandler.java"
    "CompressedIoUringLoggingHandler.java"
)

# 复制核心io_uring文件到modb-iouring
echo "复制核心io_uring文件到modb-iouring..."
if [ -d "${SOURCE_MAIN}" ]; then
    # 复制指定的核心文件
    for file in "${IOURING_FILES[@]}"; do
        if [ -f "${SOURCE_MAIN}/${file}" ]; then
            cp "${SOURCE_MAIN}/${file}" "${IOURING_MAIN}/"
            echo "✓ 复制 ${file} 到 modb-iouring"
        fi
    done
    
    # 复制util和c目录
    if [ -d "${SOURCE_MAIN}/util" ]; then
        cp -r "${SOURCE_MAIN}/util" "${IOURING_MAIN}/"
        echo "✓ 复制 util 目录到 modb-iouring"
    fi
    
    if [ -d "${SOURCE_MAIN}/c" ]; then
        cp -r "${SOURCE_MAIN}/c" "${IOURING_MAIN}/"
        echo "✓ 复制 c 目录到 modb-iouring"
    fi
    
    # 复制placeholder文件
    if [ -f "${SOURCE_MAIN}/placeholder" ]; then
        cp "${SOURCE_MAIN}/placeholder" "${IOURING_MAIN}/"
    fi
else
    echo "⚠ 警告: 源目录 ${SOURCE_MAIN} 不存在"
fi

# 复制logging文件到modb-common
echo "复制logging文件到modb-common..."
if [ -d "${SOURCE_MAIN}" ]; then
    for file in "${LOGGING_FILES[@]}"; do
        if [ -f "${SOURCE_MAIN}/${file}" ]; then
            cp "${SOURCE_MAIN}/${file}" "${COMMON_MAIN}/logging/"
            echo "✓ 复制 ${file} 到 modb-common"
        fi
    done
else
    echo "⚠ 警告: 源目录 ${SOURCE_MAIN} 不存在"
fi

# 复制测试文件到modb-iouring
echo "复制测试文件到modb-iouring..."
if [ -d "${SOURCE_TEST}" ]; then
    cp -r "${SOURCE_TEST}"/* "${IOURING_TEST}/"
    echo "✓ 已复制测试文件到 modb-iouring"
else
    echo "⚠ 警告: 测试目录 ${SOURCE_TEST} 不存在"
fi

# 更新modb-iouring中的包声明
echo "更新modb-iouring中的包声明..."
find "${IOURING_MAIN}" -name "*.java" -type f -exec sed -i 's/package dk\.ku\.di\.dms\.vms\.web_common\.iouring/package dk.ku.di.dms.vms.modb.iouring/g' {} \;
find "${IOURING_TEST}" -name "*.java" -type f -exec sed -i 's/package dk\.ku\.di\.dms\.vms\.web_common\.iouring/package dk.ku.di.dms.vms.modb.iouring/g' {} \;

# 更新modb-common中的包声明
echo "更新modb-common中的包声明..."
find "${COMMON_MAIN}/logging" -name "*.java" -type f -exec sed -i 's/package dk\.ku\.di\.dms\.vms\.web_common\.iouring/package dk.ku.di.dms.vms.modb.common.logging/g' {} \;

# 更新import语句
echo "更新import语句..."
find "${IOURING_MAIN}" -name "*.java" -type f -exec sed -i 's/import dk\.ku\.di\.dms\.vms\.web_common\.iouring/import dk.ku.di.dms.vms.modb.iouring/g' {} \;
find "${IOURING_TEST}" -name "*.java" -type f -exec sed -i 's/import dk\.ku\.di\.dms\.vms\.web_common\.iouring/import dk.ku.di.dms.vms.modb.iouring/g' {} \;
find "${COMMON_MAIN}/logging" -name "*.java" -type f -exec sed -i 's/import dk\.ku\.di\.dms\.vms\.web_common\.iouring/import dk.ku.di.dms.vms.modb.iouring/g' {} \;

echo "✓ 文件迁移完成！"

# 显示迁移结果
echo ""
echo "迁移结果:"
echo "modb-iouring 主要源文件数量: $(find "${IOURING_MAIN}" -name "*.java" -type f | wc -l)"
echo "modb-iouring 测试文件数量: $(find "${IOURING_TEST}" -name "*.java" -type f | wc -l)"
echo "modb-common logging文件数量: $(find "${COMMON_MAIN}/logging" -name "*.java" -type f 2>/dev/null | wc -l)"

echo ""
echo "接下来需要手动完成的任务:"
echo "1. 更新modb-common的pom.xml添加对modb-iouring的依赖"
echo "2. 检查和修复任何剩余的导入依赖"
echo "3. 更新其他模块中对iouring类的引用"
echo "4. 运行测试确保一切正常"

echo ""
echo "迁移脚本执行完成！" 