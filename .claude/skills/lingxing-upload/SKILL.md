---
name: lingxing-upload
description: 单独执行飞书多维表格上传步骤，支持指定日期和强制重传
disable-model-invocation: true
argument-hint: "[--date YYYY-MM-DD] [--force]"
allowed-tools: Bash
---

在项目目录 /Users/wangjianhuang/Projects/lingxing_erp_exporter 中，激活虚拟环境后执行上传命令。

激活虚拟环境：`source /Users/wangjianhuang/Projects/lingxing_erp_exporter/venv/bin/activate`

用户传入的参数：$ARGUMENTS

根据参数构造命令：
- 无参数：`python upload_to_bitable.py`
- 带 --date：`python upload_to_bitable.py --date <日期>`
- 带 --force：`python upload_to_bitable.py --force`
- 两者都有：`python upload_to_bitable.py --date <日期> --force`

执行后报告上传结果。若存在 .success 文件被跳过，说明今天已上传过，如需重传请加 --force 参数。
