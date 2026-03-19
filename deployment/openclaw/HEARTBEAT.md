## ETL 兜底巡检（每 30 分钟）

仅在北京时间 15:00-18:00 期间执行以下检查（其他时间返回 HEARTBEAT_OK）：

- 检查今日 ETL 是否已执行：`ls ~/Projects/lingxing_erp_exporter/data/processed/$(date +%Y-%m-%d)/daily_summary.json`
  - 文件存在 → HEARTBEAT_OK
  - 文件不存在且已过 16:30 → 告警"ETL 今日未执行"，并尝试执行：
    ```bash
    cd ~/Projects/lingxing_erp_exporter
    rm -f ~/.lingxing_etl_$(date +%Y-%m-%d).done
    ./run_daily.sh
    ```
  - 文件不存在但未过 16:30 → HEARTBEAT_OK（ETL 可能还在运行或尚未触发）
