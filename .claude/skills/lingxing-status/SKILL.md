---
name: lingxing-status
description: 检查领星 ETL 近期运行状态，包括成功标记、每日完成标记和最新日志
disable-model-invocation: true
allowed-tools: Bash, Glob, Read
---

检查以下信息并汇总报告：

1. **近 7 天每日完成标记**
   检查 `~/.lingxing_etl_*.done` 文件，列出哪些天已完成，哪些天缺失。

2. **近 7 天上传成功标记**
   在 `data/processed/` 下查找 `daily_summary.success` 文件，列出哪些天的数据已成功上传到飞书。

3. **最新日志（最后 30 行）**
   读取 `~/Library/Logs/lingxing-etl.log` 的最后 30 行。

4. **最新错误日志（最后 20 行）**
   读取 `~/Library/Logs/lingxing-etl-error.log` 的最后 20 行，如果文件为空或不存在则说明无错误。

5. **定时任务状态**
   运行 `launchctl list | grep com.lingxing.etl` 确认任务已加载。

最后给出一句总结：系统运行是否正常，以及是否有需要关注的问题。
