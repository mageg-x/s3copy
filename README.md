中文 | [English](https://github.com/mageg-x/s3copy/blob/main/RREADME_EN.md)

# S3Copy Tool

## 强大的S3复制工具，让数据迁移更高效

S3Copy是一个功能强大的命令行工具，专为高效复制数据到S3存储而设计。无论是从本地文件、HTTP URL还是其他S3存储，它都能提供快速、可靠的传输体验，并支持断点续传、分块上传等高级特性。

## 核心功能

- 🚀 **多源支持**：本地文件/目录、HTTP/HTTPS URL、S3存储桶之间自由复制
- 🔍 **智能跳过**：基于ETag自动跳过已存在的文件，避免重复传输
- 📦 **分块上传**：超过32MB的文件自动使用分块上传，提升大文件传输效率
- 🔄 **断点续传**：传输中断后可从中断点继续，无需重新开始
- ⚡ **并发操作**：可配置的并发上传数量，充分利用带宽资源
- 📊 **实时进度**：每秒以JSON格式输出传输进度，便于监控
- 🧠 **内存优化**：流式处理大文件，避免内存溢出问题
- 🪣 **自动建桶**：目标桶不存在时自动创建，简化操作流程
- 🔁 **智能重试**：网络故障时自动重试，重试次数可配置
- 🌐 **同源优化**：同一账户和区域的S3复制使用CopyObject API，节省带宽并提高性能

## 快速开始

### 安装步骤

```bash
# 安装依赖（如需要）
npm install

# 下载Go模块
go mod download

# 编译项目
go build -o s3copy .
```

### 环境变量配置

设置以下环境变量进行身份验证：

```bash
# 源S3配置（从S3复制时需要）
export SRC_ACCESS_KEY=你的源访问密钥
export SRC_SECRET_KEY=你的源密钥
export SRC_S3_REGION=源区域  # 可选，默认：us-east-1

# 目标S3配置（始终需要）
export DST_ACCESS_KEY=你的目标访问密钥
export DST_SECRET_KEY=你的目标密钥
export DST_S3_REGION=目标区域  # 可选，默认：us-east-1
```

## 使用示例

### 从本地文件/目录复制到S3

```bash
# 复制单个文件
s3copy --from-file /path/to/file.txt --to http://region.s3.com/oss1001

# 复制整个目录
s3copy --from-file /path/to/directory --to http://region.s3.com/oss1001
```

### 从URL复制到S3

```bash
# 准备 urls.txt 内容如下
https://dldir1.qq.com/qqfile/qq/PCQQ9.7.17/QQ9.7.17.29225.exe
https://wirelesscdn-download.xuexi.cn/publish/xuexi_android/latest/xuexi_android_10002068.apk
https://dldir1v6.qq.com/weixin/Universal/Windows/WeChatWin.exe
https://24d561-2075664011.antpcdn.com:19001/b/pkg-ant.baidu.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.55.1.101.exe

# 从HTTP/HTTPS URL复制
s3copy --from-url urls.txt --to http://region.s3.com/oss1001
```

### 从S3复制到S3

```bash
# 跨S3存储桶复制
s3copy --from-s3 http://oss1001.region.s4.comm --to http://region.s3.com/oss1001
```

## 配置选项

| 参数                | 描述                                                      | 默认值 |
|-------------------|---------------------------------------------------------|--------|
| `--T`             | 并发上传线程数量                                                | 10 |
| `--part-size`     | 分块上传大小（字节）                                              | 33554432 (32MB) |
| `-q`, `--quiet`   | 静默模式（无输出）                                               | false |
| `-v`, `--verbose` | 增加日志详细程度：-v 表示 INFO 级别，-vv 表示 DEBUG 级别，-vvv 表示 TRACE 级别 | 0 |
| `--max-retries`   | 失败上传的重试次数                                               | 3 |

### 高级用法示例

```bash
# 使用自定义设置进行复制
s3copy --from-file /large-dataset \
  --to http://region.s3.com/backup \
  --T 20 \
  --part-size 67108864 \
  --max-retries 5 \
  -vv
```

## 端点格式支持

工具支持多种端点格式：

1. **桶作为子域名**：`http://bucket.endpoint.com`
2. **桶在路径中**：`http://endpoint.com/bucket`

**示例**：
- `http://oss1001.region.s3.com`
- `http://region.s3.com/oss1001`


## 进度输出

工具每秒以JSON格式输出进度：

```json
{
  "total_size": 1048576000,  // 总字节数
  "migrated_size": 524288000, // 已传输字节数
  "total_objects": 7,         // 总对象/文件 个数
  "migrated_objects": 2,     // 已完成对象数
  "fail_objects": 0,          // 上传出错的文件个数
  "average_speed": 10485760,  // 平均速度（字节/秒）
  "progress": 50.00           // 完成百分比
}
```

## 断点续传功能

传输中断后，只需再次运行相同命令即可恢复：

1. 自动检测已传输的文件和分块
2. 跳过已完成的文件
3. 从中断的分块上传处继续

## ETag检查

工具自动比较ETag以避免不必要的传输：

- 本地文件：计算MD5哈希值
- S3对象：使用现有ETag
- HTTP源：使用响应头中的ETag（如果有）

## 大文件内存优化

处理大型HTTP文件时，工具采用以下策略：

1. **分块流式传输**：同时下载和上传，避免一次性加载整个文件
2. **内存控制**：使用固定大小的缓冲区，避免内存堆积
3. **支持续传**：可从中断点恢复下载
4. **高效缓冲**：优化缓冲区使用，平衡性能和内存占用

## 自动桶创建

如果目标桶不存在，工具会在上传前自动创建它，省去手动创建的麻烦。

## 重试机制

工具包含智能重试机制，默认情况下会重试失败的上传最多3次，每次间隔3秒。你可以通过`--max-retries`参数自定义重试次数。

## 日志级别控制

工具支持通过`-v`参数控制日志输出详细程度，不同级别的含义如下：

| 参数 | 日志级别 | 含义 |
|------|----------|------|
| 不加 `-v` | ERROR/WARN | 只输出错误或警告信息 |
| `-v` | INFO | 输出主要流程信息（如"开始上传"、"完成下载"） |
| `-vv` | DEBUG | 输出详细调试信息（如请求头、参数） |
| `-vvv` | TRACE | 输出最详细信息（如完整请求/响应体、堆栈） |

## 错误处理

工具能处理各种错误情况：

- 网络中断（带可配置的重试逻辑）
- 认证错误
- 权限问题
- 大文件内存限制
- 无效端点格式

所有错误都会记录详细信息，并更新内部状态以保持一致性。

## 性能优化提示

1. **并发上传**：增加`--T`值提高吞吐量（需平衡系统资源）
2. **分块大小**：larger分块减少API调用但增加内存使用
3. **网络选择**：使用同一区域的端点以获得更好性能
4. **内存使用**：工具设计为无论文件大小都使用最小内存
5. **重试设置**：根据网络稳定性调整`--max-retries`
6. **同源复制**：同一账户和区域内的S3到S3复制，工具会自动使用更高效的CopyObject API，节省带宽


## 许可证

AGPL 3 License