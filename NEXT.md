# 現在在跑什麼，以及接下來做什麼

寫於 2026-09-15 12:15，在 leepc 上。這份檔案給接手的 Claude Code session。
主要的機制說明在 `README.md`，改動的理由在 commit message 裡，**這裡不重複**。

## 現在的狀態

2 小時測試正在 tmux session `crawl` 裡跑。

```
tmux attach -t crawl
```

| window | 內容 |
|---|---|
| `run` | `ops/run_hour.sh 7200`，跑測本體 |
| `claude` | 空的，給你開 Claude Code |
| `watch` | 每 30 秒刷新 `data/runstats.tsv` |

跑測目錄是 `data/run-20260915-121243`，12:12:44 開始，14:12 左右結束。

**離開 tmux 用 `Ctrl-b d`，不要用 `Ctrl-c`。** `Ctrl-c` 會殺掉跑測。

## 第一個問題已經有答案了

上一輪的計畫要 2 小時測試回答「單一 reactor 執行緒是不是天花板」。
開跑 2 分鐘就確認了：

| elapsed | proc_cpu | main_cpu | lag_max | frontier | pqueues |
|---|---|---|---|---|---|
| 31s | 101.3% | 98.5% | 1,936 ms | 31,213 | 1,285 |
| 60s | 99.0% | 97.5% | 1,759 ms | 78,622 | 2,350 |
| 151s | 97.1% | 95.0% | 1,774 ms | 146,500 | 1,863 |

**主執行緒 95–98%，整個程序也只有 99%。8 核用 1 核。**
reactor 延遲最大 1.9 秒，事件迴圈確實塞住了。

所以 `README.md` 裡「reactor thread is the fourth ceiling」那一節的推論成立，
而且是直接量到的，不再是推論。

## ⚠️ 跑測中途發現的 bug，已修但跑測沒吃到

`38f179c` 修了一個靜默的 bug。**正在跑的這次測試是在修正前啟動的，所以
它的吞吐量數字偏低。**

Bug：spider 在自己的 `from_crawler` 裡讀 `crawler.bloom_dupefilter`，
但 `Crawler.crawl` 先建 spider、後建 engine，而 dupefilter 是 engine
建 scheduler 時才產生的。所以那個屬性當下不存在，`spider.dupefilter`
被設成 `None`。

**沒有任何東西壞掉。** scheduler 還是會過濾重複，爬取結果完全正確。
只是「先查 bloom 再建 Request」這個最佳化整個沒生效，白做了它要省的工。
唯一的症狀是 `dupe_skipped` 一直是 0，對照 `discovered` 已經 768,922。

修法是改在 `spider_opened` 綁定。45 秒的 smoke run 現在會跳過 8,211 個
Request。`tests/test_dupefilter.py` 多了一個案例，用真實順序建 spider 和
engine 來驗證綁定。

**所以：**

- 這次跑測的 politeness、robots、記憶體、CPU 數字**都有效**
- 它的 **pages/s 偏低**，因為每個重複 URL 還是建了 Request
- 要拿正確的吞吐量，跑完後 `git pull` 再跑一次，或直接接受它是下限

判斷要不要重跑之前，先看這次的 CPU 數字。主執行緒已經 99%，多 process
才是主要的槓桿，這個修正是次要的。

## 剩下要從這次跑測拿到的東西

| 問題 | 看哪裡 | 判準 |
|---|---|---|
| 逾時降下來了嗎 | stats dump 的 `downloader/exception_count` | 對比 response_count，要 < 10% |
| robots 逾時降了嗎 | `robotstxt/exception_count/*DownloadTimeout*` | 上次 89%，要大幅下降 |
| 去重省掉多少 | `dupefilter/skipped_before_request` | 上次同期 3.3M 個 Request |
| 記憶體收斂嗎 | `ops/report_growth.py` 的 rate last 1/3 | ≤ 0 |
| 2.5 GB 是什麼 | `data/objects.log` 前 10 名 vs RSS | 差距大代表是 malloc 碎片 |
| pop 會不會成為問題 | `runstats.tsv` 的 `pqueues` 欄 | > 10,000 就要做 B2 |
| 日誌夠安靜嗎 | `ls -la data/run-*/scrapy.log` | 上次 495 MB，要 < 5 MB |

## 抓 profile

跑測第 40 分鐘左右（約 12:52）抓一次。這是「每頁 22 ms 花在哪」的唯一直接證據。

```bash
export PATH=$HOME/.local/bin:$PATH
PID=$(pgrep -f "bin/scrapy crawl" | head -1)
sudo $HOME/.local/bin/py-spy record -p $PID -d 60 -o ~/ntu-sa-crawler/data/profile.svg
```

`sudo` 是免密碼的。注意 `pgrep` 要抓 python 那個 pid，不是 `uv run` 的父程序。

## 跑完之後

```bash
export PATH=$HOME/.local/bin:$PATH; cd ~/ntu-sa-crawler
uv run python ops/verify_politeness.py    # 必須 VIOLATIONS : 0
uv run python ops/verify_robots.py        # 必須 VIOLATIONS : 0
uv run python ops/report_metrics.py
uv run python ops/report_growth.py data/run-20260915-121243
```

**politeness 是不可妥協的。** 作業寫「NO violation」。任何改動之後都要重跑，
而且 `verify_politeness.py` 比對的是 `required_min_gap`（5.0），
不是 `download_delay`（5.1），所以調參數不會鬆綁測試。

## 下一步：多 process 分片

主執行緒已經確認飽和，所以這是唯一能再拉高吞吐量的方向。
`crawler/spiders/broad.py` 已經有 `shard` / `shards` 參數和 `_shard_of()`，
但沒有任何東西在用它。

要點：

- 每個網域只屬於一個 shard，所以 politeness 計時器不跨 process。這是
  分片安全的根本原因
- `ops/run_hour.sh` 要起 N 個 `scrapy crawl broad -a shard=i -a shards=N
  -s JOBDIR=state/job-i`
- `verify_politeness.py` 和 `report_metrics.py` 已經用 glob 讀
  `crawled*.log.gz`，合併驗證不用改
- **N 由記憶體決定**。等這次跑測的 RSS 峰值出來再算。bloom 每 process
  171 MB 是固定成本
- 跨 shard 的 URL 先照舊丟掉。Tier 1 的**發現**指標不受影響，因為
  `discovered_raw` 的累加在 shard 檢查之前

先看完這次跑測的數字再決定 N，不要直接開 8。

## 還沒做的

**Windows Update 還沒暫停。** 沒有命令列做法，要使用者手動到
設定 → Windows Update → 進階選項，暫停到 9/20 之後。更新會自動重開機，
那會殺掉 48 小時的正式跑測。

**報告本身。** 素材在 `README.md`，還沒動筆。9/19 23:59 寄到
huang.Taiyi@gmail.com。

**跑完 48 小時要還原睡眠設定**：`powercfg /change standby-timeout-ac 30`。

## 安全限制

Tailnet 上另外兩台 Ubuntu 主機
（`100.75.91.15` / hungtse-b550-aorus-master、`100.90.228.88` / hungtse-dogas）
是**公司機器，絕對不能用**。使用者明確說過「不能用那是公司的機器」。

GitHub repo 保持 **Private**。

## 使用者現在不在

使用者出門了，Mac 關機。這個 session 是接手用的。

**可以自己做的：** 讀資料、跑分析腳本、改程式碼、跑測試、commit。

**要等使用者回來才做的：**

- 開始 48 小時正式跑測。開跑時間是他的決定，截止是 9/17 00:00
- 暫停 Windows Update。沒有命令列做法
- 任何會刪掉跑測資料的事

跑測 14:12 左右結束。結束後照上面的驗收流程跑完，把結果整理好等他回來。
如果決定要重跑一次拿正確的吞吐量數字，那是可逆的，可以自己做。

## 兩個提醒

**不要只看 exit code。** 有兩次 exit code 回 0 但實際沒生效：`powercfg`
的批次指令、以及 seed 探測讀不滿一個 chunk。都是查詢驗證才發現的。

**關閉很慢。** `CONCURRENT_REQUESTS=3000` 時 Scrapy 的優雅關閉要等每個
停等請求逾時，實測超過 5 分鐘。`ops/run_hour.sh` 已經處理：先複製日誌，
再 SIGTERM，20 秒後 SIGKILL。
