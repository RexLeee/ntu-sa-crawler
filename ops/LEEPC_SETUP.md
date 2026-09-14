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

```bash
rsync -az --delete \
  --exclude '.venv' --exclude '.git' --exclude '.ruff_cache' \
  --exclude 'data/*.log.gz' --exclude '__pycache__' \
  ./ leepc-wsl:~/ntu-sa-crawler/

ssh leepc-wsl 'export PATH="$HOME/.local/bin:$PATH"; cd ~/ntu-sa-crawler && uv sync'
```

`uv` installs to `~/.local/bin`, which a non-interactive SSH session does not
put on PATH, so every remote command needs the explicit `export` above.

## Host facts

| Item | Value |
|---|---|
| CPU visible to WSL | 16 logical cores |
| RAM visible to WSL | 7.7 GB (WSL2 defaults to 50% of the 15.9 GB host) |
| Disk | 952 GB free |
| WSL distro | Ubuntu 26.04.1 LTS, systemd enabled |
| System Python | 3.14.4 (the project pins its own via uv) |
| eth0 MTU | 1280; raising it to 1500 broke connectivity, so leave it |

To give WSL more RAM, create `C:\Users\lkj20\.wslconfig`:

```ini
[wsl2]
memory=12GB
processors=8
```

Then `wsl --shutdown` and reconnect.

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
