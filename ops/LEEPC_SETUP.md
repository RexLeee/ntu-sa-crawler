# Running on leepc (Windows 11 + WSL2)

The crawl host is a Windows 11 machine reached over Tailscale. The work runs
inside its WSL2 Ubuntu 26.04 distro.

## Why WSL needs its own sshd

`wsl.exe` cannot be driven over a Tailscale SSH session. Every invocation,
including `wsl --help`, exits 1 with no output, because it needs an
interactive desktop session token that the SSH session does not have. The
Windows SSH user (`rex`) is also not the desktop user (`lkj20`), so it has no
distro of its own registered.

The fix is to give WSL its own sshd and forward a port to it, so the Windows
layer is bypassed entirely.

## One-time setup

### 1. Inside WSL (run from a desktop WSL window)

```bash
sudo apt update && sudo apt install -y openssh-server

sudo sed -i 's/^#\?Port .*/Port 2222/' /etc/ssh/sshd_config
sudo sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo sed -i 's/^#\?PubkeyAuthentication .*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

mkdir -p ~/.ssh && chmod 700 ~/.ssh
echo '<client public key>' >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# /etc/wsl.conf already has systemd=true, so this survives restarts
sudo systemctl enable ssh
sudo systemctl start ssh
```

### 2. On Windows (Administrator PowerShell)

WSL2 gets a new NAT address on every boot, so the forward has to be rebuilt
each time rather than hardcoded.

```powershell
@'
$wslIp = (wsl -d Ubuntu -e hostname -I).Trim().Split()[0]
netsh interface portproxy delete v4tov4 listenport=2222 listenaddress=0.0.0.0 2>$null
netsh interface portproxy add v4tov4 listenport=2222 listenaddress=0.0.0.0 connectport=2222 connectaddress=$wslIp
'@ | Set-Content C:\Users\lkj20\wsl-portproxy.ps1

powershell -ExecutionPolicy Bypass -File C:\Users\lkj20\wsl-portproxy.ps1
New-NetFirewallRule -DisplayName "WSL SSH 2222" -Direction Inbound -LocalPort 2222 -Protocol TCP -Action Allow

# rebuild the forward automatically after a reboot
$action  = New-ScheduledTaskAction -Execute "powershell.exe" `
           -Argument "-ExecutionPolicy Bypass -File C:\Users\lkj20\wsl-portproxy.ps1"
$trigger = New-ScheduledTaskTrigger -AtStartup
Register-ScheduledTask -TaskName "WSL SSH portproxy" -Action $action -Trigger $trigger `
           -User "SYSTEM" -RunLevel Highest -Force
```

### 3. Keep the machine awake

A 48 hour run cannot survive a sleep or a forced update reboot.

```powershell
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
powercfg /change monitor-timeout-ac 0
powercfg /change disk-timeout-ac 0
```

Pause Windows Update through Settings until after the run ends.

### 4. Client SSH config

```
Host leepc-wsl
    HostName 100.84.218.75
    Port 2222
    User rex
    ServerAliveInterval 30
    ServerAliveCountMax 6
```

Verify with `ssh leepc-wsl 'nproc && free -h'`.

## Deploying code

The host pulls from GitHub with a repo-scoped deploy key. That beats rsync for
a 48 hour run: parameters can be changed and rolled back from either side, and
the deployed tree has a commit id.

Generate the key in WSL and register the public half at
`https://github.com/RexLeee/ntu-sa-crawler/settings/keys/new` with
**Allow write access** ticked:

```bash
ssh-keygen -t ed25519 -N "" -C "leepc-wsl-deploy" -f ~/.ssh/id_ed25519_ntu_sa
cat ~/.ssh/id_ed25519_ntu_sa.pub
```

Point git at it, then clone:

```bash
cat >> ~/.ssh/config <<'EOF'
Host github.com
    IdentityFile ~/.ssh/id_ed25519_ntu_sa
    IdentitiesOnly yes
EOF
chmod 600 ~/.ssh/config

ssh -T git@github.com     # expect "successfully authenticated"
git clone git@github.com:RexLeee/ntu-sa-crawler.git ~/ntu-sa-crawler
cd ~/ntu-sa-crawler && uv sync
```

Afterwards a deploy is `git pull && uv sync`.

`uv` installs to `~/.local/bin`, which a non-interactive SSH session does not
put on PATH, so every remote command needs an explicit
`export PATH="$HOME/.local/bin:$PATH"`.

## Long runs

`ops/run_hour.sh` raises the fd limit, starts the crawl, and samples RSS,
thread count, open fds and established sockets every 30s into
`data/run-<stamp>/resources.tsv`.

```bash
ssh leepc-wsl 'cd ~/ntu-sa-crawler && nohup bash ops/run_hour.sh 3600 \
    > /tmp/run_hour.out 2>&1 &'
```

`ops/report_growth.py <rundir>` then fits the memory and domain curves against
both `t` and `sqrt(t)`. That distinction is the point of a long run: linear
memory growth means the single-process design cannot reach 48 hours, while
sub-linear growth means it can. The same fit on the domain curve says whether
the frontier has stopped growing, which is what makes a rate extrapolation
meaningful at all.

## Host facts

| Item | Value |
|---|---|
| Windows | build 22000, 21H2 |
| CPU | AMD Ryzen 7 3700X, 8 cores / 16 threads |
| Host RAM | 15.95 GB |
| Disk | one volume (C:), 161 GB free; WSL rootfs 952 GB free |
| WSL | 2.7.14.0, kernel 6.18.33.2 |
| WSL distro | Ubuntu 26.04.1 LTS, systemd enabled |
| System Python | 3.14.4 (the project pins its own via uv) |
| eth0 MTU | 1280; raising it to 1500 broke connectivity, so leave it |
| Ephemeral ports | 32768-60999 (~28k) |

Windows 21H2 is below 22H2, so `networkingMode=mirrored`, `dnsTunneling` and
`firewall` are unavailable. That costs nothing here: the workload is outbound
HTTP from Linux, which NAT already handles, and mirrored mode has known DNS
and ephemeral-port failures that a 48 hour run cannot absorb.

## The file descriptor limit, not memory, is the first ceiling

`ulimit -n` is 1024 by default. `CONCURRENT_REQUESTS=200` plus DNS sockets and
the gzip log handles approaches that, so raising concurrency hits
"too many open files" before it hits memory or bandwidth.

The hard limit is 1048576, so no root and no config file are needed. The run
script raises the soft limit itself:

```bash
ulimit -n 65536
```

## Resource tuning

`C:\Users\lkj20\.wslconfig` (create it; it does not exist by default):

```ini
[wsl2]
memory=10GB
processors=8
swap=4GB

[experimental]
autoMemoryReclaim=gradual
sparseVhd=true
```

| Setting | Why |
|---|---|
| `memory=10GB` | Up from the 7.7 GB default (50% of host). This is a ceiling, not a reservation. Above 12 GB the host starts paging, which makes the crawl slower rather than faster. |
| `processors=8` | Down from 16. The crawler waits on sockets, not on CPU, so the spare threads buy nothing and starve the Windows desktop. |
| `swap=4GB` | A safety net. Without swap the OOM killer ends a 48 hour run outright. |
| `autoMemoryReclaim=gradual` | The default `dropCache` drops page cache whenever the VM idles, which penalises the crawler every time it waits on network I/O and resumes. |
| `sparseVhd=true` | Lets the VHDX shrink again instead of growing permanently. |

Deliberately omitted: `pageReporting` no longer exists (WSL 2.0.7+ reports
`Unknown key`; `autoMemoryReclaim` replaced it), `vmIdleTimeout=-1` is not a
documented value, and `swapFile` has nowhere better to live on a single-volume
host.

Inside the distro, stop Linux from swapping out the active frontier:

```bash
echo 'vm.swappiness=10' | sudo tee /etc/sysctl.d/99-crawler.conf
sudo sysctl -p /etc/sysctl.d/99-crawler.conf
```

### Applying it

`.wslconfig` needs the whole VM to restart. `wsl --terminate` is not enough:
it stops a distro without tearing down the VM.

```powershell
wsl --shutdown
Start-Sleep -Seconds 10
wsl -d Ubuntu -e true
powershell -ExecutionPolicy Bypass -File C:\Users\lkj20\wsl-portproxy.ps1
```

This must be run at the machine. `wsl --shutdown` stops the WSL sshd too, and
nothing restarts WSL on its own, so an SSH session cannot recover itself. The
NAT address also changes, which is why the portproxy is rebuilt afterwards.

Verify with:

```bash
ssh leepc-wsl 'free -h; nproc; swapon --show'
```

## Bandwidth

Measurements against a single CDN are misleading here: Cloudflare's local edge
reported 132 Mbps while returning HTTP 429 under repeated testing. Against
international targets the link delivers about 6-11 Mbps, and the Mac on a
different network measured the same order.

What matters for a crawler is pages per second on small HTML documents, and
that is latency-bound rather than bandwidth-bound: 10 concurrent fetches gave
7.3 pages/s, and 60 concurrent gave 8.1 pages/s against the same targets.

The real crawler does far better than that probe because it spreads requests
across thousands of domains rather than reusing a handful.
