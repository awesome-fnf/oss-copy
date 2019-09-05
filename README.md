## 简介

本示例演示了如何使用函数工作流从一个 OSS Bucket 复制文件到另一个 Bucket，其中源 Bucket 和目标 Bucket 可以在同一个区域也可以在不同区域。


## 场景

### 增量数据复制
增量数据复制依赖于函数计算的 OSS 触发器，当有新文件上传或者文件更新时，OSS 会触发函数 `startCopyWithFnF`，该函数启动一个 FnF 流程，逻辑如下：
* 如果要复制的文件比较小（比如 50MB 以内），会使用一个函数流式下载文件并上传文件到目标 Bucket。
* 如果要复制的文件比较大（比如 50MB 到 1GB），会使用一个函数多线程分片下载文件并分片上传到目标 Bucket。
* 如果要复制的文件很大（比如 1GB 以上），会使用三个函数完成复制任务。第一个函数开始分片上传，第二个函数的多个实例分片下载并分片上传到目标 Bucket，第三个函数完成分片上传。

其中文件大小阈值可以根据具体情况配置，原则是函数执行不超过最长执行时间限制（10分钟）。
* 同区域复制延迟低，文件大小阈值可以适当调大。
* 跨区域复制延迟高，文件大小阈值可以适当调小。

**相关配置**
* 源 Bucket `src_bucket` 由 Funcraft template 配置。
* OSS 触发器配置由 Funcraft template 配置。
* 目的 Bucket `dst_bucket` 由 `startCopyWithFnF` 函数的环境变量配置
* 源 OSS endpoint `src_bucket_endpoint` 和目的 OSS endpoint `dst_bucket_endpoint` 通过函数环境变量配置
* 分片大小 `part_size` 在 FnF 流程定义中配置
* 文件大小阈值 `small_threshold` `large_threshol` 在 FnF 流程定义中配置

**使用步骤**

1. 使用[Funcraft](https://help.aliyun.com/document_detail/64204.html)部署函数

    ```fun deploy -t template.yml```

2. 使用[阿里云 CLI](https://help.aliyun.com/document_detail/122611.html) 创建流程。使用控制台请参见[文档](https://help.aliyun.com/document_detail/124155.html)。流程定义使用[incremental.yaml](./flows/incremental.yaml)。

    ```aliyun fnf CreateFlow --Description "incremental copy" --Type FDL --Name oss-incremental-copy --Definition "$(<./flows/incremental.yaml)" --RoleArn acs:ram::account-id:role/fnf```

3. 测试复制文件：使用[阿里云 CLI](https://help.aliyun.com/document_detail/122611.html) 执行流程。使用控制台请参见[文档](https://help.aliyun.com/document_detail/124156.html)。执行使用下面的输入格式。该输入将会处理 `hangzhouhangzhou` 的所有文件。

    ```aliyun fnf StartExecution --FlowName oss-incremental-copy --Input '{"src_bucket": "hangzhouhangzhou", "dest_bucket": "svsvsv", "key": "tbc/Archive.zip", "total_size": 936771720}' --ExecutionName run1```

4. 使用 ossutil 上传文件到源 Bucket，该文件会被同步到目的 Bucket。

    ```ossutil -e http://oss-cn-hangzhou.aliyuncs.com -i ak -k secret  cp ~/Downloads/testfile oss://hangzhouhangzhou/tbc/```

### 存量数据复制

## 优势
* 支持任意大小文件的复制。
* 可靠的复制：依赖于函数工作流的重试功能。

## 扩展场景
* 将文件复制到多个区域：只需要修改流程定义添加一个 `foreach` 步骤，对每个区域执行复制逻辑。
* 不受限制解压缩任意大小文件：只需要按照文件大小采用不同的解压方式。
    * 解压后是数量较少的文件
    * 解压后是数量较多的文件

## 优化
* 检查 crc 确保数据的完整性
* 大文件分片后，目前是一个函数实例处理一个分片，可以由一个函数实例处理多个分片，消除目前 FnF 并行循环步骤的 100 并行数限制。
* 不通过 init 步骤方式配置流程使用常量，减少步骤转换数。