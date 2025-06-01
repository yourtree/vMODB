# IoUring 迁移指南

本指南说明如何将现有的系统从传统的文件IO迁移到高性能的io_uring实现。

## 概述

我们提供了一套完整的IoUring替换方案，可以在不修改现有代码的情况下启用io_uring功能：

- **日志层面**: `IoUringLoggingHandler` 和 `CompressedIoUringLoggingHandler`
- **存储层面**: `IoUringRecordBufferContext`
- **事务管理**: 通过上述组件自动获得io_uring支持

## 快速开始

### 方法1: 程序化配置（推荐）

在应用启动时调用以下代码：

```java
import dk.ku.di.dms.vms.modb.common.config.IoUringBootstrap;

public class Main {
    public static void main(String[] args) {
        // 启用所有IoUring功能
        IoUringBootstrap.enableAll();
        
        // 或者启用带压缩的IoUring
        // IoUringBootstrap.enableAllWithCompression();
        
        // 启动您的应用...
    }
}
```

### 方法2: 系统属性配置

设置以下系统属性：

```bash
# 启用IoUring
-Diouring.enabled=true
-Dstorage_backend=iouring
-Dlogging_type=iouring

# 或者启用压缩日志
-Diouring.enabled=true
-Dstorage_backend=iouring
-Dlogging_type=compressed_iouring
```

### 方法3: 应用配置文件

在您的`app.properties`文件中添加：

```properties
# IoUring配置
iouring.enabled=true
storage_backend=iouring
logging_type=iouring

# 可选：启用默认压缩
iouring.enable_by_default=true
iouring.compression_enabled=true
```

## 配置选项

### 存储后端选项

- `default`: 使用传统的文件映射
- `iouring`: 使用io_uring异步IO

### 日志类型选项

- `default`: 使用`DefaultLoggingHandler`
- `compressed`: 使用`CompressedLoggingHandler`
- `iouring`: 使用`IoUringLoggingHandler`
- `compressed_iouring`: 使用`CompressedIoUringLoggingHandler`

## 选择性启用

### 仅启用存储IoUring

```java
IoUringBootstrap.enableStorageOnly();
```

### 仅启用日志IoUring

```java
IoUringBootstrap.enableLoggingOnly();
```

### 仅启用压缩日志IoUring

```java
IoUringBootstrap.enableCompressedLoggingOnly();
```

## 禁用IoUring

```java
IoUringBootstrap.disableAll();
```

## 检查配置状态

```java
IoUringBootstrap.printStatus();
```

输出示例：
```
=== IoUring Bootstrap Status ===
IoUring Configuration:
  Global enabled: true
  Storage backend: iouring (enabled: true)
  Logging type: compressed_iouring (enabled: true)
==============================
```

## 兼容性

### 自动回退

如果io_uring初始化失败，系统会自动回退到传统实现：

```java
// 在IoUringRecordBufferContext初始化失败时
System.err.println("Failed to create IoUring RecordBufferContext, falling back to default");
// 自动使用RecordBufferContext

// 在IoUringLoggingHandler初始化失败时
System.err.println("Failed to create IoUring LoggingHandler, falling back to default");
// 自动使用DefaultLoggingHandler
```

### 现有代码兼容性

所有现有的API调用保持不变：

```java
// 这些调用不需要修改
TransactionManager txManager = new TransactionManager(catalog, checkpointing);
RecordBufferContext bufferContext = StorageUtils.loadRecordBuffer(...);
ILoggingHandler logger = LoggingHandlerBuilder.build(identifier);

// IoUring支持是透明的
```

## 性能优势

使用io_uring可以带来以下性能提升：

1. **异步IO**: 减少线程阻塞时间
2. **批量操作**: 提高IO吞吐量
3. **减少系统调用**: 降低内核切换开销
4. **更好的缓存利用**: 改善内存访问模式

## 故障排除

### 常见问题

1. **库文件缺失**: 确保`modb-iouring`模块已正确编译并包含native库
2. **权限问题**: 确保应用有足够权限访问io_uring系统调用
3. **内核版本**: 确保Linux内核版本支持io_uring（推荐5.1+）

### 调试日志

启用详细日志：

```java
IoUringBootstrap.initialize(); // 会打印当前配置状态
```

### 测试IoUring功能

运行测试验证io_uring是否正常工作：

```bash
# 运行IoUring存储测试
./gradlew :modb:test --tests "*IoUringStorageTest*"

# 运行IoUring日志测试
./gradlew :modb:test --tests "*IoUringLoggingTest*"
```

## 迁移检查清单

- [ ] 确认系统支持io_uring
- [ ] 编译包含native库的modb-iouring模块
- [ ] 选择合适的配置方法（程序化/系统属性/配置文件）
- [ ] 在应用启动时启用IoUring
- [ ] 运行测试验证功能正常
- [ ] 监控性能指标确认改进效果
- [ ] 准备回退方案以防出现问题

## 示例应用配置

### marketplace应用

在`marketplace/*/src/main/resources/app.properties`中添加：

```properties
# 启用IoUring
iouring.enabled=true
storage_backend=iouring
logging_type=compressed_iouring
```

### 自定义应用

```java
public class MyApplication {
    public static void main(String[] args) {
        // 在任何数据库操作之前启用IoUring
        IoUringBootstrap.enableAllWithCompression();
        
        // 现有的应用逻辑保持不变
        VmsApplication app = VmsApplication.builder()
            .withCheckpointing(true)
            .withLogging(true)
            .build();
        
        app.start();
    }
}
``` 