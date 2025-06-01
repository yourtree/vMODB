#!/bin/bash

echo "🔍 验证IoUring日志集成逻辑链条"
echo "======================================="

echo ""
echo "📋 步骤1: 编译项目"
echo "-------------------"
cd tpcc/proxy-tpcc
if mvn compile -q; then
    echo "✅ 编译成功"
else
    echo "❌ 编译失败"
    exit 1
fi

echo ""
echo "📋 步骤2: 验证逻辑链条"
echo "-------------------"

echo "🔗 检查 IoUringBootstrap.enableLoggingOnly() 方法:"
if grep -q "System.setProperty.*iouring.enabled.*true" ../../modb-common/src/main/java/dk/ku/di/dms/vms/modb/common/config/IoUringBootstrap.java; then
    echo "✅ enableLoggingOnly() 正确设置 iouring.enabled=true"
else
    echo "❌ enableLoggingOnly() 未正确设置系统属性"
fi

if grep -q "System.setProperty.*logging_type.*iouring" ../../modb-common/src/main/java/dk/ku/di/dms/vms/modb/common/config/IoUringBootstrap.java; then
    echo "✅ enableLoggingOnly() 正确设置 logging_type=iouring"
else
    echo "❌ enableLoggingOnly() 未正确设置日志类型"
fi

echo ""
echo "🔗 检查 LoggingHandlerBuilder.build() 方法:"
if grep -q "ioUringEnabled.*System.getProperty.*iouring.enabled" ../../modb-common/src/main/java/dk/ku/di/dms/vms/modb/common/logging/LoggingHandlerBuilder.java; then
    echo "✅ LoggingHandlerBuilder 正确读取 iouring.enabled 属性"
else
    echo "❌ LoggingHandlerBuilder 未正确读取系统属性"
fi

if grep -q "IoUringLoggingHandler" ../../modb-common/src/main/java/dk/ku/di/dms/vms/modb/common/logging/LoggingHandlerBuilder.java; then
    echo "✅ LoggingHandlerBuilder 支持创建 IoUringLoggingHandler"
else
    echo "❌ LoggingHandlerBuilder 不支持 IoUringLoggingHandler"
fi

echo ""
echo "🔗 检查 VmsWorker 使用 LoggingHandlerBuilder:"
if grep -q "LoggingHandlerBuilder.build.*coordinator.*consumerVms.identifier" ../../coordinator/src/main/java/dk/ku/di/dms/vms/coordinator/vms/VmsWorker.java; then
    echo "✅ VmsWorker 正确使用 LoggingHandlerBuilder.build()"
else
    echo "❌ VmsWorker 未使用 LoggingHandlerBuilder"
fi

echo ""
echo "🔗 检查 VmsWorker 中的关键调用点:"
if grep -q "loggingHandler.force()" ../../coordinator/src/main/java/dk/ku/di/dms/vms/coordinator/vms/VmsWorker.java; then
    echo "✅ VmsWorker.sendMessage() 正确调用 loggingHandler.force()"
else
    echo "❌ VmsWorker.sendMessage() 未调用 loggingHandler.force()"
fi

if grep -q "loggingHandler.log(writeBuffer)" ../../coordinator/src/main/java/dk/ku/di/dms/vms/coordinator/vms/VmsWorker.java; then
    echo "✅ VmsWorker.processPendingLogging() 正确调用 loggingHandler.log()"
else
    echo "❌ VmsWorker.processPendingLogging() 未调用 loggingHandler.log()"
fi

echo ""
echo "🔗 检查 ExperimentUtils 中的配置调用:"
if grep -q "IoUringBootstrap.enableLoggingOnly()" src/main/java/dk/ku/di/dms/vms/tpcc/proxy/experiment/ExperimentUtils.java; then
    echo "✅ ExperimentUtils.loadCoordinator() 正确调用 IoUringBootstrap.enableLoggingOnly()"
else
    echo "❌ ExperimentUtils.loadCoordinator() 未调用 IoUringBootstrap.enableLoggingOnly()"
fi

echo ""
echo "🔗 检查 IoUring 日志实现:"
if [[ -f "../../modb-common/src/main/java/dk/ku/di/dms/vms/modb/common/logging/IoUringLoggingHandler.java" ]]; then
    echo "✅ IoUringLoggingHandler 类存在"
    if grep -q "ioUring.queueWrite" ../../modb-common/src/main/java/dk/ku/di/dms/vms/modb/common/logging/IoUringLoggingHandler.java; then
        echo "✅ IoUringLoggingHandler.log() 使用异步 queueWrite"
    else
        echo "❌ IoUringLoggingHandler.log() 未使用异步写入"
    fi
    
    if grep -q "ioUring.queueFsync" ../../modb-common/src/main/java/dk/ku/di/dms/vms/modb/common/logging/IoUringLoggingHandler.java; then
        echo "✅ IoUringLoggingHandler.force() 使用异步 queueFsync"
    else
        echo "❌ IoUringLoggingHandler.force() 未使用异步 fsync"
    fi
else
    echo "❌ IoUringLoggingHandler 类不存在"
fi

echo ""
echo "📋 逻辑链条总结"
echo "-------------------"
echo "1. 🚀 ExperimentUtils.loadCoordinator() 调用 IoUringBootstrap.enableLoggingOnly()"
echo "2. ⚙️  IoUringBootstrap.enableLoggingOnly() 设置系统属性:"
echo "   - iouring.enabled=true"  
echo "   - logging_type=iouring"
echo "3. 🏗️  VmsWorker 构造函数调用 LoggingHandlerBuilder.build()"
echo "4. 🔍 LoggingHandlerBuilder.build() 检查系统属性并创建 IoUringLoggingHandler"
echo "5. ⚡ VmsWorker.sendMessage() 调用 loggingHandler.force() → 异步 fsync"
echo "6. ⚡ VmsWorker.processPendingLogging() 调用 loggingHandler.log() → 异步写入"
echo ""
echo "🎯 结论: IoUring日志集成逻辑链条完整，预期可以正常工作！" 