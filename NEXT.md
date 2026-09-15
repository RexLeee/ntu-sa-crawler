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

### 驗證結果（`data/run-20260915-164523`，32,000 個請求）

| 檢查 | 結果 |
|---|---|
| `verify_politeness.py` | **VIOLATIONS : 0**，最小間隔 5.000s |
| `verify_robots.py` | VIOLATIONS : 0 |
| `violation-trace.log` | **0 bytes** |
| pages/s | 54.54（上次 51.7） |

同樣 32,000 個請求，上一次是 6 次違規。

### 順帶修掉的第二個缺陷，以及還沒修的那半

meta refresh 也繼承了 `dont_filter`。spider 對已經過 bloom 的請求設這個旗標，
所以 meta refresh 的目標**跳過了去重**。
`config.toml` 的 `follow_meta_refresh = false` 把這條路關掉。

**但重複抓取只少了一部分，沒有歸零。** 同樣 32,000 次抓取：504 → 451 次浪費。

剩下的是 HTTP 轉址，那是轉址的性質，不是這份程式碼的缺陷。
轉址目標不會經過 spider 的過濾器，因為 spider 只測它從頁面抽出來的 URL。
所以多個不同的 URL 可以塌縮到同一個目標：

```
https://parklogic.com/index.html   302 ->  https://parklogic.com/
https://parklogic.com/Services     302 ->  https://parklogic.com/
```

每個來源都合法地通過了過濾器，然後各自把同一個目標再抓一次。
**只花吞吐量，不影響合規**：每次抓取仍然走該網域的計時器，
所以 `verify_politeness.py` 維持 0。

佔 1.4%，優先序低於多核心。要修的話是在轉址中介層測一次 bloom。

## 多核心分片做完了，但結論是負面的（`14b8290`、`82487e0`）

分片實作完成、測過、驗證合規，**但它沒有變快，所以預設關掉**。

| | 單 process | 4 分片 |
|---|---|---|
| pages/s | **54.54** | 52.29 |
| 第 570 秒發出的請求 | 94,305 | **162,721** |
| 第 570 秒的回應 | 44,986 | 46,144 |
| 回應率 | 48% | **28%** |
| TCP 連線 | ~1,700 | ~1,600 |
| DNS 快取 | 25,567 | ~24,700 |
| CPU | 單執行緒 99% | 每片 23–41% |

**四個 process 發出 1.7 倍的請求，拿回一樣多的回應。**
TCP 連線數和 DNS 快取兩邊一樣，這就是關鍵證據：限制的是這條網路和解析器
能承載多少連線，不是 CPU。多開 process 只是多發會逾時的請求。

CPU 那一列是佐證。分片之後沒有任何 process 接近飽和，
代表**上一輪 reactor 執行緒根本就不是瓶頸**。它看起來像瓶頸是因為
`pop()` 真的在燒它，修好之後限制換了地方，但沒有人重新量過換到哪裡。

所以 `sharding.shards = 1`，`[memory]` 全部改回單 process 的值。
**機制保留不動**，因為它在任何分片數都合規，而且如果哪天網路不再是瓶頸，
它是唯一能用上更多頻寬的結構。

### 中途踩到的兩個坑

**`concurrent_requests` 不能除以分片數。** 第一次分片跑測是 45.66 pages/s，
比單 process 還慢，因為這個值留在 3000。它是「停在網域計時器上的請求」的預算，
所以限制的是同時在飛的網域數。一個 shard 只有四分之一的網域，
預算就被堆在它持有的少數網域上。改成 12000 才回到 52.29。

**`report_metrics.py` 少算四分之三的發現數而且不報錯。**
`zcat -f a b c d` 讀到第一個被 SIGKILL 截斷的檔案就停了，後面三個完全沒讀，
透過管線之後 exit code 還是 0。單一檔案時無害，四個檔案就吃掉四分之三：
報 116,000，實際 448,000。改成每個檔案各自一個 `zcat`。

## 還沒做的

**1. 例外率。** 需要獨立調查。大部分可能是 robots 嚴格模式的刻意拒絕，
那是刻意的行為，不是故障。要先把例外分類才知道。

**2. Windows Update 還沒暫停。** 沒有命令列做法，要使用者手動到
設定 → Windows Update → 進階選項，暫停到 9/20 之後。更新會自動重開機。

**3. 報告本身。** 素材在 `README.md`，還沒動筆。9/19 23:59 寄到
huang.Taiyi@gmail.com。

**4. 48 小時正式跑測。** 9/17 00:00 前要開跑。開跑時間是使用者的決定。

**5. 跑完 48 小時要還原睡眠設定**：`powercfg /change standby-timeout-ac 30`。

## 測試

36 個檢查，兩台機器都跑過：

```bash
export PATH=$HOME/.local/bin:$PATH; cd ~/ntu-sa-crawler
uv run python tests/test_handoff.py       # 6 個檢查
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
uv run python ops/verify_no_refetch.py    # 目前約 1.4%，見上面的轉址說明
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

**修掉一個瓶頸之後，要重新量下一個在哪裡。** 這輪整個分片工作是建立在
「CPU 還是飽和的」這個觀察上。那個觀察沒錯，但它是 `pop()` 造成的殘影：
修好之後真正的限制已經換成網路，只是沒有人重新量。
分片做完才發現四個 process 都只用 23–41% CPU，那一刻才知道方向錯了。
**一個工作做完是負面結果，不代表白做，但可以更早知道。**

**負面結果也是證據。** 追蹤器「什麼都沒記錄」而驗證器報六次違規，
這個矛盾本身就是答案：它直接證明問題不在計時精度，而在分組。
設計診斷時要想清楚「沒響」代表什麼。

**修在對的那一層。** 上一次在 `RedirectMiddleware` 裡重設 slot，
那是治症狀，所以 meta refresh 又把同樣的 bug 帶回來。
會複製 meta 的路沒有完整清單，所以要修的是「不要存身分」這件事本身。

**修 bug 要先證明測試抓得到舊的 bug。** `foreign_meta` 情境對舊程式碼
兩個斷言都失敗，間隔是 0.001s 對 2.0s 下限。`verify_no_refetch.py` 也對
舊資料驗證過，抓到 414 個重複 URL。
