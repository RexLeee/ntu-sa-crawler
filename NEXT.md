# 現在的狀態，以及接下來做什麼

寫於 2026-09-15 16:50。機制說明在 `README.md`，每個改動的理由在 commit
message 裡，**這裡不重複**。

## 一句話

pop 的 CPU 問題和 politeness 違規都修好了。下一步是多核心分片，
等使用者確認 politeness 乾淨之後才開始。

## pop 已經修好並驗證（`fb67eba`）

`crawler/pqueue.py` 的 `RingDownloaderAwarePriorityQueue` 取代了
Scrapy 的 `DownloaderAwarePriorityQueue`。選擇規則沒變，只是不再每次掃全部網域。

| 項目 | 修前 | 修後 |
|---|---|---|
| `pop` 總時間 | 50.5% | **3.8%** |
| `stats` | 31.7% | 消失 |
| `_active_downloads` | 20.4% | 消失 |
| `parse` | 5.1% | 21.7% |
| pages/s | 23.0 | **51.7** |
| reactor 延遲 p50 | 1,200–2,000 ms | **1–58 ms** |

**瓶頸換人了。** 現在最貴的是 `parse` 和 `_extract_links`，合計約 42%。
那是爬蟲真正需要做的工作，所以單 process 已經沒有便宜的空間。

## politeness 已經修好（`6093310`）

### 症狀與根因

10 分鐘跑測 32,000 個請求：`verify_politeness.py` 報 6 次違規，
但派送當下的追蹤器**一個字都沒寫**。

這兩件事同時成立只有一種解釋：計時器確實把每一對隔開了 5 秒，
**但它分錯組了**。

違規的 URL 都不在 `discovered.log.gz` 裡，代表 spider 從沒 yield 過它們。
它們是 `MetaRefreshMiddleware` 造出來的。那個中介層和 `RedirectMiddleware`
共用 `_build_redirect_request`，用 `source_request.replace()` 整份複製 meta，
所以目標請求帶著**來源網域**的 `download_slot`：

```
av.jpn.support.panasonic.com  ->  panasonic.jp
www.home-assistant.io         ->  www.openhomefoundation.org
```

兩個來源頁都用 curl 確認過，都是 200 加上 `<meta http-equiv="refresh">`。

### 為什麼上一次的修法沒擋住

`RedirectMiddleware` 早就被改成會重設 slot（README 有一節寫這個）。
**那個修法治的是症狀。** 它只修好一條路，下一條複製 meta 的路就把同樣的
bug 帶回來。而「會複製 meta 的路」沒有一份會保持完整的清單。

### 修法：身分不要存，一律從 URL 算

`crawler/downloader.py` 的 `DomainSlotDownloader.get_slot_key` 從 URL 算，
那是每個下載都會經過的唯一一點，用的是 `verify_politeness.py` 同一個
`slot_key()`。計時器和驗證器不可能再分歧，而且沒有存起來的值就沒有東西可以繼承。

`dispatch.py` 同理，不再讀 meta。`SlotKeyMiddleware` 因此多餘，已刪除。
spider、robots、redirect 三處寫 meta 的地方一併刪掉。

### 順帶修掉的第二個缺陷

meta refresh 也繼承了 `dont_filter`。spider 對已經過 bloom 的請求設這個旗標，
所以 meta refresh 的目標**跳過了去重**。上一次跑測有 414 個 URL 被抓超過一次，
浪費 504 次抓取。`ops/verify_no_refetch.py` 現在會抓這個，
`config.toml` 的 `follow_meta_refresh = false` 把這條路關掉。

## 還沒做的

**1. 多核心分片。** 計畫已寫好，等使用者確認 politeness 之後開始。
`crawler/spiders/broad.py` 已有 `shard` / `shards` / `_shard_of()`，沒有東西在用。

關鍵是**跨片 URL 必須交接**，不能像現在直接丟掉。broad crawl 的新網域幾乎
全靠跨網域連結發現，丟掉會讓每片的活躍網域數掉回單 process 的水準。

**N 由記憶體決定。** 每片約 1.2–1.5 GB，WSL2 有 9.9 GB。**先開 4 量一次再決定**，
不要直接開 8。`[memory]` 的每個上限都要除以 N。

**2. 例外率 84%。** 需要獨立調查。大部分可能是 robots 嚴格模式的刻意拒絕，
那是刻意的行為，不是故障。要先把例外分類才知道。

**3. Windows Update 還沒暫停。** 沒有命令列做法，要使用者手動到
設定 → Windows Update → 進階選項，暫停到 9/20 之後。更新會自動重開機。

**4. 報告本身。** 素材在 `README.md`，還沒動筆。9/19 23:59 寄到
huang.Taiyi@gmail.com。

**5. 48 小時正式跑測。** 9/17 00:00 前要開跑。開跑時間是使用者的決定。

**6. 跑完 48 小時要還原睡眠設定**：`powercfg /change standby-timeout-ac 30`。

## 測試

30 個檢查，兩台機器都跑過：

```bash
export PATH=$HOME/.local/bin:$PATH; cd ~/ntu-sa-crawler
uv run python tests/test_dispatch.py      # 5 個情境
uv run python tests/test_pqueue.py        # 7 個檢查
uv run python tests/test_scheduler.py     # 5 個檢查
uv run python tests/test_robots_cache.py  # 7 個檢查
uv run python tests/test_dupefilter.py    # 6 個檢查
uvx ruff check crawler/ tests/ ops/
```

`tests/test_pqueue.py --parent` 會對 Scrapy 的原類別跑同一批行為檢查。

## 驗收

```bash
export PATH=$HOME/.local/bin:$PATH; cd ~/ntu-sa-crawler
uv run python ops/verify_politeness.py    # 必須 VIOLATIONS : 0
uv run python ops/verify_robots.py        # 必須 VIOLATIONS : 0
uv run python ops/verify_no_refetch.py    # 必須 REPEATS : 0
uv run python ops/report_metrics.py
```

還要看 `data/violation-trace.log`。**它現在必須是 0 bytes。**
以前它和驗證器分組方式不同，所以空的不代表沒問題；現在兩邊同一個定義，
裡面有東西就是真的違規。

**politeness 不可妥協。** 作業寫「NO violation」。`verify_politeness.py`
比對的是 `required_min_gap`（5.0），不是 `download_delay`（5.1），
所以調參數不會鬆綁測試。

## 跑測的方式

**一律用 tmux 在 leepc 跑。** 使用者可能隨時關掉 Mac。

```bash
/tmp/wsl_ssh.sh 'tmux new-session -d -s verify -c ~/ntu-sa-crawler \
  "export PATH=\$HOME/.local/bin:\$PATH; ops/run_hour.sh 600 > /tmp/verify-run.log 2>&1"'
```

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

⚠️ `-r 250` 會讓 py-spy 跟不上，用 100 以下。

## 四個提醒

**不要相信推論，去量。** 上一輪從延遲推論出 parse 是瓶頸，寫進 README
當結論，profile 顯示它只佔 5.2%。飽和的系統裡每個症狀都指向所有原因。

**負面結果也是證據。** 追蹤器「什麼都沒記錄」而驗證器報六次違規，
這個矛盾本身就是答案：它直接證明問題不在計時精度，而在分組。
設計診斷時要想清楚「沒響」代表什麼。

**修在對的那一層。** 上一次在 `RedirectMiddleware` 裡重設 slot，
那是治症狀，所以 meta refresh 又把同樣的 bug 帶回來。
會複製 meta 的路沒有完整清單，所以要修的是「不要存身分」這件事本身。

**修 bug 要先證明測試抓得到舊的 bug。** `foreign_meta` 情境對舊程式碼
兩個斷言都失敗，間隔是 0.001s 對 2.0s 下限。`verify_no_refetch.py` 也對
舊資料驗證過，抓到 414 個重複 URL。
