---
name: lingxing-export
description: 手动触发领星数据完整 ETL 流程（下载 → 筛选 → 预处理 → 聚合 → 上传飞书）
disable-model-invocation: true
allowed-tools: Bash
---

在项目目录 /Users/wangjianhuang/Projects/lingxing_erp_exporter 中，激活虚拟环境后依次执行以下 5 个步骤，每步完成后报告结果，任意步骤失败则立即停止并报告错误：

1. 下载原始数据：`python main.py`
2. 筛选产品数据：`python process.py`
3. 预处理数据：`python preprocess.py`
4. 聚合数据：`python aggregate.py`
5. 上传到飞书多维表格：`python upload_to_bitable.py`

激活虚拟环境的命令：`source /Users/wangjianhuang/Projects/lingxing_erp_exporter/venv/bin/activate`

全部完成后，汇总每步耗时和结果。
