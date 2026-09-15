# 現在的狀態，以及接下來做什麼

寫於 2026-09-15 15:40。機制說明在 `README.md`，每個改動的理由在 commit
message 裡，**這裡不重複**。

## 一句話

pop 的 CPU 問題解決了。politeness 還剩一個未確認根因的違規，正在用生產環境
追蹤抓它。多核心要等 politeness 確定乾淨之後才動。

## pop 已經修好並驗證（`fb67eba`）

`crawler/pqueue.py` 的 `RingDownloaderAwarePriorityQueue` 取代了
Scrapy 的 `DownloaderAwarePriorityQueue`。選擇規則沒變，只是不再每次掃全部網域。

profile 比對，同一台機器同樣設定：

| 項目 | 修前 | 修後 |
|---|---|---|
| `pop` 總時間 | 50.5% | **3.8%** |
| `stats` | 31.7% | 消失 |
| `_active_downloads` | 20.4% | 消失 |
| `_next_slot` | 10.6% | 消失 |
| `parse` | 5.1% | 21.7% |
| `_extract_links` | 4.9% | 20.9% |

跑測層級：

| 項目 | 2 小時跑測 | 修後 10 分鐘 |
|---|---|---|
| pages/s | 23.0 | **37.6** |
| reactor 延遲 p50 | 1,200–2,000 ms | **15–430 ms** |
| 主執行緒 CPU | 99% | 91–99% |

延遲降一個數量級，是 pop 讓出 CPU 最直接的證據。

**瓶頸換人了。** 現在最貴的是 `parse` 和 `_extract_links`，合計 21.7%。
那是爬蟲真正需要做的工作，不是浪費。

## politeness：一個違規，根因未確認

10 分鐘跑測，22,000 個請求裡 1 次：

```
panasonic.jp   gap=0.000s
```

兩個 URL 同一毫秒派送。該網域前面安靜了 116 秒。robots 是 0 違規。

### 已排除的八個假設

每一個都用真實的 `crawler/dispatch.py` 寫了重現腳本，全部得到正確的 5 秒間隔：

slot 回收後 `id()` 重用（這是上次修好的 bug，已改用網域字串）、事件迴圈阻塞、
第三個請求覆蓋兄弟預約、轉址繼承 meta、robots.txt 抓取路徑、10 萬筆清除掃描、
seed 注入、共用 meta dict。

`Downloader._download` 只有一個呼叫點，所以兩個請求都確實經過了計時器。

### 已經做的（`074c4dc`、`a4c819d`）

**1. 預約只能往前走。** 第二次寫入原本無條件覆蓋：

```python
next_allowed[slot_id] = dispatched + min_gap          # 舊
next_allowed[slot_id] = max(next_allowed.get(slot_id, 0.0),
                            dispatched + min_gap)      # 新
```

協程睡著時，兄弟可能已經預約了更晚的時段。舊的寫法會用較小的值蓋掉它，
下一個到達的請求就拿到兄弟正在睡的那個時段。

**這是真實的危險，但無法證明它就是觀察到那一對的原因。**

**2. 生產環境追蹤。** `config.toml` 的 `trace_short_gaps = true`。
任何低於下限的派送對會把完整狀態寫進 `data/violation-trace.log`：
兩個 URL、預約與實際時間、字典值、slot 物件的 id / delay / lastseen /
active / transferring / queue、slot 是否還是活的、meta、完整堆疊。

每次派送兩個 dict 操作，只在間隔過短時寫，48 小時都可以開著。

**3. 回歸測試。** `tests/test_dispatch.py` 第四個情境直接斷言不變量：
字典裡的值不曾下降。輸掉競態的交錯依賴事件迴圈時序，測試無法可靠重現，
所以測性質不測 staged 失敗。

### 現在在跑

`data/run-20260915-153104`，15:31 開始的 10 分鐘帶追蹤跑測。

| 結果 | 下一步 |
|---|---|
| 0 違規、trace 空 | 再跑一次 10 分鐘確認，然後 1 小時 |
| 0 違規、trace 有紀錄 | 讀 trace 確認機制，寫進 README |
| 仍有違規 | 照 trace 的堆疊修 |

## 新增的量測欄位

`runstats.tsv` 多了 `requests`、`responses`、`exceptions` 三欄。
例外率原本只存在於 stats dump，而 SIGKILL 結束的跑測不會寫那份。

已經有用：這次跑測第 151 秒例外率 83.7%。對照上一次同期 102%
（例外數超過請求數，因為 robots 拒絕與重試各自計數）。
**這是既有問題，不是 pop 修改造成的，而且比以前好。**

## 還沒做的

**1. 確認 politeness 乾淨。** 最優先。上面在跑。

**2. 多核心分片。** 等 politeness 確認之後規劃。
`crawler/spiders/broad.py` 已有 `shard` / `shards` 參數和 `_shard_of()`，
沒有東西在用。每個網域只屬於一個 shard，所以計時器不跨 process，
這是分片安全的根本原因。

**先把單 process 的合規做對，再乘以 N。** 分片會把任何殘留的競態乘以 N 倍。

**3. 例外率 84%。** 需要獨立調查。大部分可能是 robots 嚴格模式的拒絕，
那是刻意的行為，不是故障。要先把例外分類才知道。

**4. Windows Update 還沒暫停。** 沒有命令列做法，要使用者手動到
設定 → Windows Update → 進階選項，暫停到 9/20 之後。更新會自動重開機。

**5. 報告本身。** 素材在 `README.md`，還沒動筆。9/19 23:59 寄到
huang.Taiyi@gmail.com。

**6. 48 小時正式跑測。** 9/17 00:00 前要開跑。開跑時間是使用者的決定。

**7. 跑完 48 小時要還原睡眠設定**：`powercfg /change standby-timeout-ac 30`。

## 測試

28 個檢查，兩台機器都跑過：

```bash
export PATH=$HOME/.local/bin:$PATH; cd ~/ntu-sa-crawler
uv run python tests/test_dispatch.py      # 4 個情境
uv run python tests/test_pqueue.py        # 6 個檢查
uv run python tests/test_scheduler.py     # 5 個檢查
uv run python tests/test_robots_cache.py  # 7 個檢查
uv run python tests/test_dupefilter.py    # 6 個檢查
uvx ruff check crawler/ tests/ ops/
```

`tests/test_pqueue.py --parent` 會對 Scrapy 的原類別跑同一批行為檢查，
五個都通過。這證明新類別保留了語意，不是自說自話。

## 抓 profile

```bash
export PATH=$HOME/.local/bin:$PATH
PID=$(pgrep -f "venv/bin/python.*scrapy crawl" | head -1)
sudo $HOME/.local/bin/py-spy record -p $PID -d 45 -r 100 -f speedscope -o /tmp/prof.speedscope
```

⚠️ `pgrep -f "bin/scrapy crawl"` 會抓到 `uv run` 的包裝程序，py-spy 對它會報
「Failed to find python version」。要用上面那個比較精確的樣式。

⚠️ **用 pattern 判斷跑測是否結束會騙人。** 你自己的檢查指令的命令列裡
也含有那個 pattern，`pgrep` 會匹配到自己。用 `kill -0 <pid>` 比較可靠。
`ops/run_hour.sh` 本身是記錄 pid 的，沒有這個問題。

⚠️ `-r 250` 會讓 py-spy 跟不上，用 100 以下。

## 驗收

```bash
export PATH=$HOME/.local/bin:$PATH; cd ~/ntu-sa-crawler
uv run python ops/verify_politeness.py    # 必須 VIOLATIONS : 0
uv run python ops/verify_robots.py        # 必須 VIOLATIONS : 0
uv run python ops/report_metrics.py
uv run python ops/report_growth.py data/run-<stamp>
```

**politeness 不可妥協。** 作業寫「NO violation」。`verify_politeness.py`
比對的是 `required_min_gap`（5.0），不是 `download_delay`（5.1），
所以調參數不會鬆綁測試。

## 環境

**leepc**（正式跑的機器）

- Tailscale `100.114.150.20`，帳號 `rex`，port 2222
- 連線用 `/tmp/wsl_ssh.sh`（本機 helper，不在 repo）
- 非互動 shell 要 `export PATH=$HOME/.local/bin:$PATH`
- Python 3.14.4，Scrapy 2.19，Ubuntu 26.04，AMD Ryzen 7 3700X 8 核
- WSL2 配置 10 GB 記憶體、8 核、4 GB swap，磁碟剩 950 GB
- `sudo` 免密碼，`py-spy` 已裝

## 安全限制

Tailnet 上另外兩台 Ubuntu 主機
（`100.75.91.15` / hungtse-b550-aorus-master、`100.90.228.88` / hungtse-dogas）
是**公司機器，絕對不能用**。使用者明確說過「不能用那是公司的機器」。

GitHub repo 保持 **Private**。

## 三個提醒

**不要相信推論，去量。** 上一輪從延遲推論出 parse 是瓶頸，寫進 README
當結論，profile 顯示它只佔 5.2%。飽和的系統裡每個症狀都指向所有原因。

**不要只看 exit code。** 已經有四次「成功但沒生效」：`powercfg` 批次指令、
dupefilter 綁定、第一版的 slot GC 測試、以及 `pgrep` 匹配到自己。
都是查詢驗證才發現的。

**修 bug 要先證明測試抓得到舊的 bug。** 兩次都這樣做，兩次都發現第一版
測試是無效的。這次的預約競態無法 staged，所以改成斷言不變量，並在
commit message 裡寫明這個限制。
