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

## ⚠️ 跑測之間的變異很大，單次數字不可信

改回 `shards = 1` 之後的回歸跑測（`data/run-20260915-192616`）：

| | 基準 `164523` | 回歸 `192616` |
|---|---|---|
| pages/s | 54.54 | **68.16** |
| 抓到的網域 | 2,229 | **1,031** |
| 其中種子網域 | 622 / 1000 | **390 / 1000** |
| 發現的 URL | 486,000 | 350,000 |
| 回應率 | 48% | 64% |

**pages/s 高了 25%，但那不是改善。** 這次只跑起來 390 個種子網域，
基準是 622 個。種子起不來就沒有連結，沒有連結就沒有新網域，
於是預算都花在少數已知網站上鑽得更深。回應率變高也是同一個原因：
碰到的是比較穩定的那批站。

兩次跑測的**程式碼在吞吐量路徑上沒有差別**，config 的數值也全部一樣
（diff 只有新增的鍵）。所以這個差距來自外部：DNS、網路狀況、
對方網站當下的可用性。

**結論：10 分鐘跑測之間的變異大到可以蓋過 25% 的差距。**
不要用單次 10 分鐘跑測比較效能。要比就跑同樣長度、連續跑幾次取中位數，
或者直接看 1 小時以上。

分片那張比較表（下一節）也受這個限制影響。它的結論
「網路是瓶頸」有 TCP 連線數和 DNS 快取兩個獨立證據支撐，
比單看 pages/s 可靠，但 52.29 對 54.54 這個差距本身在噪音範圍內。

## 還沒做的

**1. 例外率。** 需要獨立調查。大部分可能是 robots 嚴格模式的刻意拒絕，
那是刻意的行為，不是故障。要先把例外分類才知道。

現在有資料可以做這件事了。`exception_type_count/*` 在 stats dump 裡，
而 stats dump 以前每次都被 SIGKILL 吃掉。`crawler/extensions/shutdown.py`
讓 crawl 自己在 `RUN_DURATION` 到時關閉，`runstats.py` 另外每 30 秒把
整包 stats 寫成 `data/stats-N.json` 當備援。

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
uv run python tests/test_dispatch.py      # 3 個情境 + 守門員 + 分組鍵
uv run python tests/test_downloader.py    # 延遲從回應結束起算，這是核心不變量
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
# 預設讀 data/dispatched*.log.gz，涵蓋 robots.txt 與失敗的請求
uv run python ops/verify_politeness.py                    # 必須 VIOLATIONS : 0
uv run python ops/verify_politeness.py --source crawled   # 必須 VIOLATIONS : 0
uv run python ops/verify_robots.py --hosts 100            # 必須 VIOLATIONS : 0
uv run python ops/verify_no_refetch.py    # 目前約 1.4%，見上面的轉址說明
uv run python ops/report_metrics.py --json data/run-<stamp>/metrics.json
uv run python ops/report_timeline.py data/run-<stamp>
```

乾淨關閉與存檔完整性，每一項都要過：

```bash
# stats.json 裡的 finish_reason 才是乾淨關閉的證據。
# "Dumping Scrapy stats" 那行是 INFO，log_level 是 WARNING，永遠不會出現。
grep -h finish_reason data/run-*/stats-*.json            # 每個 shard 都要 run_duration
ls state/job-*/requests.bloom                            # 每個 shard 一個
cat data/run-*/supervisor.log                            # crash 必須 0；self-close 可接受
tail -3 data/run-*/resources.tsv                         # 看 RSS 與 disk_free_mb
```

`supervisor.log` 的判準改了。`finish_reason=memusage_exceeded` 是乾淨關閉，
supervisor 會重啟那個 shard，log 裡寫 `closed itself`。這是健康事件：
bloom 已存檔、frontier 在磁碟上，新 process 的 robots cache 歸零。
`died rc=` 才是缺陷，必須是 0。`run_hour.sh` 的 close check 會分開報。

跨 shard 交接必須真的在運作，這是一小時 run 唯一漏掉的檢查：

```bash
grep -o '"handoff/[a-z_]*": [0-9]*' data/run-*/stats-*.json
grep -c AsyncioLoopingCall data/run-*/scrapy-*.log       # 必須是 0
grep -o '"spider_exceptions[^,]*' data/run-*/stats-*.json # 不該出現 ValueError
du -sh state/handoff/to-*                                 # 不該是整場 run 的量
```

- 一邊的 `handoff/sent` 要約等於另一邊的 `handoff/received`。
  一小時 run 是 1,791,515 對 181,164，因為兩個 loop 在前九分鐘就死了。
- `AsyncioLoopingCall` 的錯誤訊息出現一次就代表交接已經停了，整場都不會恢復。
- `handoff/bad_url` 可以大於 0，那是 backstop 在工作；但 loop 必須活到結束。

frontier 與檔案描述符要一起看，它們是同一條曲線：

```bash
for f in data/runstats-*.tsv; do tail -1 $f | cut -f14,15; done  # frontier, pqueues
tail -1 data/run-*/resources.tsv | cut -f5                       # fds
```

frontier 到 `frontier_max_size`（3M）之後，`pqueues` 與 `fds` 都必須持平。
繼續線性上升就是上限沒生效。`scheduler/dropped/over_capacity` 大於 0
才證明上限真的在作用。

兩小時 run 證明「frontier 持平」不等於「pqueues 持平」。請求數釘在 3,000,810，
queue 數還是從 46,788 長到 132,779，因為新 domain 的第一個 URL 豁免請求上限。
所以要另外看 queue 數的上限：

```bash
grep -o '"scheduler/dropped/over_queues": [0-9]*' data/run-*/stats-*.json
for f in data/runstats-*.tsv; do tail -1 $f | cut -f15; done   # 必須停在 40,000
awk -F'\t' 'NR>1{if($10>a)a=$10; if($11>b)b=$11} END{print a/1024, b/1024}' \
  data/run-*/resources.tsv                                    # 每個 shard < 2,400 MB
```

被 kill 之後必須能續跑。這是兩小時 run 最嚴重的缺陷，重啟的 process
讀到空的 frontier，1.66 秒就結束：

```bash
grep -E "resumed a frontier of|rebuilt .*active.json" data/run-*/scrapy-*.log
ls data/run-*/stats*.restart*.json        # 有重啟才會有，內容是死掉那一輪
ls -d state/job-*.broken-*                # 不該存在；存在代表 frontier 被丟掉
find state/job-0/requests.queue -name info.json | head -3   # run 中途就要有
```

- 重啟後第一列 runstats 的 `frontier` 要接近被殺前的值，不能是 0。
- `supervisor.log` 的 `stopped itself ... not restarting` 是 `finished`，
  那不是缺陷，但它代表那個 shard 提早沒工作了，要在報告裡說明。
- 磁碟速率要在上限生效之後才量，而且要用 30 分鐘以上的區間：
  兩個 shard 合計應低於 500 MB/h。用整場平均會把填充期混進來。

還要看兩個東西，兩個都必須是空的：

- `data/violation-trace-*.log` **必須是 0 bytes**。
- `data/stats-*.json` 裡的 `dispatch/guard_refused` **必須是 0 或不存在**。

這兩個現在是同一件事。`crawler/dispatch.py` 的守門員只要看到間隔小於 5.0 秒，
就寫一筆 trace、加一次 `guard_refused`，並且**丟掉那個請求**。
節流由 `crawler/downloader.py` 做，它從「上一個回應結束」起算，
所以守門員在設計上不可能觸發。觸發就是有 bug，要先查清楚再跑正式的。

trace 的門檔現在固定是 `required_min_gap`，不再隨 robots.txt 的 Crawl-delay
浮動，所以不需要看 `applicable_floor` 那一欄了。網站要求的 Crawl-delay
寫進 `slot.delay`，由同一個「回應結束起算」的機制保證。

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
