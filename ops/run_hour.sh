#!/usr/bin/env bash
# Long-duration run with resource sampling.
#
# Short samples (5-11 min) are all still in ramp-up: the domain frontier is
# still growing when they end, so their pages/sec cannot be extrapolated.
# This samples resources on a fixed interval so the growth curve can be
# plotted, not just its endpoint.
#
# Starts sharding.shards processes, each owning a disjoint set of domains.
# Every per-domain rate limit is therefore enforced inside one process and
# needs no coordination between them. See crawler/handoff.py.
#
# The crawl stops itself: each shard gets -s RUN_DURATION and closes cleanly
# from the inside, which is what writes the stats dump and persists the Bloom
# filter. See crawler/extensions/shutdown.py for why a signal from outside
# could not do it.
#
# Each shard runs under a restart loop. A shard that dies resumes its own
# JOBDIR, so it keeps its frontier and its dupefilter; restarts are recorded in
# <rundir>/supervisor.log, which must be empty for a run to be reported as
# uninterrupted.
#
# Usage: ops/run_hour.sh [duration_seconds] [seeds_file]
#
#   ops/run_hour.sh 1200                 # 20 minute smoke
#   ops/run_hour.sh 172800               # the 48 hour run
#   RESUME=1 ops/run_hour.sh 86400       # continue on the existing frontier
#
# Environment overrides: SHARDS, SAMPLE_INTERVAL, SHUTDOWN_GRACE,
# MIN_DISK_FREE_MB, MAX_RESTARTS, RESUME.
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"

# The soft limit is 1024. Each (domain, priority) pair in the frontier is a
# FifoDiskQueue holding two open files, head and tail, so descriptors track
# the frontier's shape rather than the number of live connections: a measured
# run reached 7,569 queues and 15,422 descriptors. Raising the soft limit to
# the hard limit needs no root and no config file.
#
# Take the hard limit rather than a fixed number: it was 1,048,576 on WSL but
# pam_limits sets it per host, and under `set -e` a bare `ulimit -n 1000000`
# would abort the run before the first page if the ceiling were lower.
FD_HARD=$(ulimit -Hn)
ulimit -n "$FD_HARD" 2>/dev/null || true

# glibc raises its mmap threshold dynamically as a program frees large blocks,
# after which page bodies of a few hundred KB are served from the heap and
# fragment it instead of being returned to the OS. Pinning the threshold and
# capping arenas keeps RSS closer to the live set. data/objects.log shows
# whether it worked: flat object counts against rising RSS mean fragmentation.
export MALLOC_ARENA_MAX=2
export MALLOC_MMAP_THRESHOLD_=131072

# tldextract fetches the Public Suffix List on first use, and crawler/slot.py
# calls it from the reactor thread on the first dispatch: a blocking HTTP
# request inside the event loop, with a silent fallback to the bundled snapshot
# if it fails. The snapshot and the live list can disagree about private
# suffixes, and slot_key() is the rate-limit unit, so that would change which
# hosts share a 5 second timer. Pin the cache so every run keys domains the
# same way. ops/GCP_SETUP.md populates it once, before the first crawl.
export TLDEXTRACT_CACHE="${TLDEXTRACT_CACHE:-$PWD/state/tldextract}"

DURATION="${1:-3600}"
SEEDS="${2:-seeds/seeds_1000.txt}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-30}"
# How long to wait after RUN_DURATION for the in-crawl shutdown to finish. It
# cancels the parked requests, so what remains is bounded by download_timeout
# plus one per-domain gap. 600 is generous; the elapsed time is printed.
SHUTDOWN_GRACE="${SHUTDOWN_GRACE:-600}"
# Abort rather than let the disk fill. A full disk fails the gzip writers and
# the JOBDIR write path at once, which kills both shards mid-run.
MIN_DISK_FREE_MB="${MIN_DISK_FREE_MB:-3000}"
# A shard that dies is restarted on the same JOBDIR, so it resumes its frontier
# and its Bloom filter. The cap stops a crash loop from running all night.
MAX_RESTARTS="${MAX_RESTARTS:-10}"
# RESUME=1 keeps the previous run's frontier and logs. For restarting a crawl
# whose supervisor died, not for a fresh measurement.
RESUME="${RESUME:-0}"
# Single source of truth is config.toml, so the workers and the verifier can
# never disagree about how many shards exist.
SHARDS="${SHARDS:-$(grep -E '^shards[[:space:]]*=' config.toml | head -1 | tr -dc '0-9')}"
SHARDS="${SHARDS:-1}"
STAMP=$(date +%Y%m%d-%H%M%S)
RUNDIR="data/run-${STAMP}"
mkdir -p "$RUNDIR"

# Refuse to start on top of a live crawl. A previous supervisor that died
# without running its trap leaves its workers running, and they keep their
# JOBDIR open: the cleanup below then fails, and the new run either inherits a
# half-deleted frontier or dies partway through clearing it. Both happened.
#
# Matching on JOBDIR= rather than "scrapy crawl" keeps this from matching the
# grep itself or an unrelated scrapy process.
if pgrep -f "JOBDIR=state/job" >/dev/null 2>&1; then
    echo "a crawl is already running; stop it first:" >&2
    pgrep -af "JOBDIR=state/job" >&2
    exit 1
fi

if [ "$RESUME" = "1" ]; then
    echo "RESUME=1: keeping the existing frontier, bloom filter and logs"
    if ! ls -d state/job-* >/dev/null 2>&1; then
        echo "nothing to resume: no state/job-* directory exists" >&2
        exit 1
    fi
else
    # Archive rather than delete. These files used to be removed unconditionally
    # at startup, before anything checked whether the previous run had been
    # copied into its data/run-* directory. A run whose supervisor died without
    # firing its EXIT trap leaves its only copy here, and the next invocation
    # destroyed it with no prompt and no backup.
    if ls data/crawled*.log.gz data/discovered*.log.gz >/dev/null 2>&1; then
        ORPHAN="data/orphan-$(date +%Y%m%d-%H%M%S)"
        mkdir -p "$ORPHAN"
        mv data/crawled*.log.gz data/discovered*.log.gz "$ORPHAN/" 2>/dev/null || true
        mv data/dispatched*.log.gz data/runstats*.tsv data/objects*.log \
           data/stats*.json data/violation-trace*.log "$ORPHAN/" 2>/dev/null || true
        echo "previous logs moved to $ORPHAN"
    fi
    rm -f data/dispatched*.log.gz data/runstats*.tsv data/objects*.log data/stats*.json
    # The trace is opened in append mode, so a previous run's contents would be
    # read as this one's. It is now a pass/fail signal rather than a diagnostic:
    # the timer and ops/verify_politeness.py group by the same key, so anything
    # written here is a real short gap.
    rm -f data/violation-trace*.log
    # The handoff inboxes are offsets into append-only files. A stale one would
    # make a worker skip past URLs it never saw, or replay ones it already did.
    # `|| true` because a failure here must be reported by the check below rather
    # than killing the script halfway through the deletion.
    rm -rf state/job state/job-* state/handoff || true
    if ls -d state/job state/job-* state/handoff >/dev/null 2>&1; then
        echo "could not clear the frontier; something still holds it:" >&2
        ls -d state/job state/job-* state/handoff >&2
        exit 1
    fi
fi

echo "run dir      : $RUNDIR"
echo "duration     : ${DURATION}s"
echo "shards       : $SHARDS"
echo "seeds        : $SEEDS ($(grep -c '^https' "$SEEDS" 2>/dev/null || echo 0) urls)"
echo "fd limit     : $(ulimit -n)"
echo "started      : $(date -Iseconds)"

# Everything needed to tie the archived data back to the code and settings that
# produced it. The report has to describe a specific configuration, and an
# archived run used to carry no record of which one it was.
{
    echo "started       : $(date -Iseconds)"
    echo "duration_s    : $DURATION"
    echo "shards        : $SHARDS"
    echo "seeds         : $SEEDS"
    echo "seed_count    : $(grep -c '^https' "$SEEDS" 2>/dev/null || echo 0)"
    echo "fd_limit      : $(ulimit -n)"
    echo "host          : $(uname -a)"
    echo "cpus          : $(nproc 2>/dev/null || echo '?')"
    echo "memory        : $(free -m 2>/dev/null | awk '/^Mem:/{print $2" MB"}' || echo '?')"
    echo "disk          : $(df -h . | tail -1)"
    echo "python        : $(uv run python -V 2>&1)"
    echo "scrapy        : $(uv run python -c 'import scrapy;print(scrapy.__version__)' 2>&1)"
    echo "git_commit    : $(git rev-parse HEAD 2>/dev/null || echo 'not a git repo')"
} > "$RUNDIR/manifest.txt"
cp config.toml pyproject.toml uv.lock "$RUNDIR/" 2>/dev/null || true
cp "$SEEDS" "$RUNDIR/seeds_used.txt" 2>/dev/null || true

# RUN_DURATION stops the crawl from inside, which is what produces the stats
# dump, the persisted Bloom filter and the final runstats sample. Scrapy's own
# CLOSESPIDER_TIMEOUT cannot: its close waits for every request parked on a
# per-domain timer, which took over five minutes. See
# crawler/extensions/shutdown.py.
#
# Each shard runs under a supervisor loop. A shard that dies used to be
# invisible: any_alive() returned true while the other one lived, so half the
# domain space was abandoned for the rest of the run with nothing logged.
SUPERVISOR_LOG="$RUNDIR/supervisor.log"
: > "$SUPERVISOR_LOG"

run_shard() {
    local i="$1" restarts=0 started elapsed remaining rc
    local jobdir="state/job-${i}"
    local shard_started
    shard_started=$(date +%s)

    while :; do
        elapsed=$(( $(date +%s) - shard_started ))
        remaining=$(( DURATION - elapsed ))
        if [ "$remaining" -le 30 ]; then
            return 0
        fi

        started=$(date +%s)
        set +e
        uv run scrapy crawl broad \
            -a "seeds=${SEEDS}" -a "shard=${i}" -a "shards=${SHARDS}" \
            -s "JOBDIR=${jobdir}" -s "RUN_DURATION=${remaining}" \
            --logfile "$RUNDIR/scrapy-${i}.log" >/dev/null 2>&1
        rc=$?
        set -e

        # A clean stop leaves at least 30s unused only if it ran its full
        # remaining time, so treat rc 0 as done regardless of the clock.
        if [ "$rc" -eq 0 ]; then
            echo "$(date -Iseconds) shard=$i exited cleanly rc=0" >> "$SUPERVISOR_LOG"
            return 0
        fi

        restarts=$((restarts + 1))
        local ran=$(( $(date +%s) - started ))
        echo "$(date -Iseconds) shard=$i died rc=$rc after ${ran}s, restart $restarts/$MAX_RESTARTS" \
            >> "$SUPERVISOR_LOG"

        if [ "$restarts" -ge "$MAX_RESTARTS" ]; then
            echo "$(date -Iseconds) shard=$i giving up after $restarts restarts" \
                >> "$SUPERVISOR_LOG"
            return 1
        fi

        # Dying twice inside two minutes means the state it resumes from is the
        # problem, not the crawl. Move it aside and let the shard cold start;
        # losing one shard's frontier beats losing the shard.
        if [ "$ran" -lt 120 ]; then
            echo "$(date -Iseconds) shard=$i died in ${ran}s, moving $jobdir aside" \
                >> "$SUPERVISOR_LOG"
            mv "$jobdir" "${jobdir}.broken-$(date +%s)" 2>/dev/null || true
        fi
        sleep 5
    done
}

PIDS=()
for i in $(seq 0 $((SHARDS - 1))); do
    run_shard "$i" &
    # $! rather than ${PIDS[-1]}: negative subscripts need bash 4.3, and the
    # Mac this is developed on ships bash 3.2, where it aborts under `set -u`.
    SHARD_PID=$!
    PIDS+=("$SHARD_PID")
    echo "shard $i pid : $SHARD_PID (supervisor)"
done

# Copy the logs into the run directory whatever happens, including Ctrl-C or
# the machine being shut down. A previous run lost its final statistics
# because this only ran on the clean-exit path.
# Defined above the trap that uses it: the recorded pid is the supervisor
# subshell, whose child is `uv run`, whose child is python. Walk the whole tree
# rather than a fixed depth.
descendants() {
    local root="$1" kid
    echo "$root"
    for kid in $(pgrep -P "$root" 2>/dev/null); do
        descendants "$kid"
    done
}

collect() {
    cp data/crawled*.log.gz data/discovered*.log.gz "$RUNDIR/" 2>/dev/null || true
    cp data/dispatched*.log.gz "$RUNDIR/" 2>/dev/null || true
    cp data/runstats*.tsv data/objects*.log "$RUNDIR/" 2>/dev/null || true
    cp data/stats*.json "$RUNDIR/" 2>/dev/null || true
    cp data/violation-trace*.log "$RUNDIR/" 2>/dev/null || true
}

# The workers must not outlive this script. A supervisor that dies on an
# unexpected error used to leave four crawls running, which then held their
# JOBDIR open and corrupted the next run's frontier. `uv run` is the parent,
# so its child has to be killed too.
reap() {
    local pid kid
    for pid in "${PIDS[@]:-}"; do
        # Kill the whole tree: the supervisor subshell, its `uv run`, and the
        # python process under that. Killing only the recorded pid used to
        # leave crawls running, which then held their JOBDIR open.
        for kid in $(descendants "$pid" 2>/dev/null | sed "1!G;h;$!d"); do
            kill -9 "$kid" 2>/dev/null || true
        done
    done
}
trap 'collect; reap' EXIT INT TERM

tree_rss() {
    local total=0 pid rss
    for pid in $(descendants "$1"); do
        rss=$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
        total=$((total + ${rss:-0}))
    done
    echo "$total"
}

# The python process is the deepest one, and the only one whose thread and fd
# counts mean anything.
leaf_pid() {
    local pid last="$1"
    for pid in $(descendants "$1"); do
        last="$pid"
    done
    echo "$last"
}

any_alive() {
    local pid
    for pid in "${PIDS[@]}"; do
        kill -0 "$pid" 2>/dev/null && return 0
    done
    return 1
}

# A shard that exits while the others run used to be invisible. The supervisor
# restarts a crash, so a dead supervisor means it gave up or finished.
all_alive() {
    local pid
    for pid in "${PIDS[@]}"; do
        kill -0 "$pid" 2>/dev/null || return 1
    done
    return 0
}

disk_free_mb() {
    df -Pm . 2>/dev/null | awk 'NR==2{print $4+0}'
}

{
    # One rss column per shard, so a shard that grows faster than the others
    # is visible rather than hidden in the total. That is what decides how
    # many shards this machine can hold.
    header='ts\telapsed\trss_kb\tthreads\tfds\ttcp_est\tcrawled_bytes\tdiscovered_bytes\tdisk_free_mb'
    for i in $(seq 0 $((SHARDS - 1))); do header="${header}\trss_kb_${i}"; done
    printf "${header}\n"
    START=$(date +%s)
    # Outlast RUN_DURATION so the in-crawl shutdown is sampled too, and so the
    # loop does not exit while the shards are still writing their stats dump.
    DEADLINE=$((DURATION + SHUTDOWN_GRACE))
    while any_alive; do
        NOW=$(date +%s)
        if [ $((NOW - START)) -ge "$DEADLINE" ]; then
            echo "sampler deadline reached (${DEADLINE}s)" >&2
            break
        fi
        # A supervisor that exits before the duration has given up on its
        # shard. Nothing used to notice; the run continued at half capacity.
        if ! all_alive && [ $((NOW - START)) -lt "$DURATION" ]; then
            echo "$(date -Iseconds) a shard supervisor exited early, stopping the run" \
                >> "$SUPERVISOR_LOG"
            echo "a shard supervisor exited early; see $SUPERVISOR_LOG" >&2
            break
        fi
        FREE_MB=$(disk_free_mb)
        if [ -n "$FREE_MB" ] && [ "$FREE_MB" -lt "$MIN_DISK_FREE_MB" ]; then
            echo "$(date -Iseconds) disk free ${FREE_MB}MB below ${MIN_DISK_FREE_MB}MB, stopping" \
                >> "$SUPERVISOR_LOG"
            echo "disk free ${FREE_MB}MB below ${MIN_DISK_FREE_MB}MB, stopping the run" >&2
            break
        fi
        TOTAL=0
        PER_SHARD=""
        for pid in "${PIDS[@]}"; do
            RSS=$(tree_rss "$pid")
            TOTAL=$((TOTAL + RSS))
            PER_SHARD="${PER_SHARD}\t${RSS}"
        done
        LEAF=$(leaf_pid "${PIDS[0]}")
        THR=$(awk '/^Threads:/{print $2}' "/proc/$LEAF/status" 2>/dev/null || echo 0)
        # Same pipefail hazard as the stat calls below: ls fails once the
        # process exits, and ss fails if the tool is briefly unavailable.
        FDS=$(ls "/proc/$LEAF/fd" 2>/dev/null | wc -l || true)
        TCP=$(ss -tn state established 2>/dev/null | tail -n +2 | wc -l || true)
        # `|| true` is load-bearing. stat exits non-zero while the glob still
        # matches nothing, which it does for the first seconds of every run,
        # and `set -o pipefail` would then take the whole script down before
        # the first sample was ever written.
        CB=$(stat -c %s data/crawled*.log.gz 2>/dev/null | awk '{s+=$1} END{print s+0}' || true)
        DB=$(stat -c %s data/discovered*.log.gz 2>/dev/null | awk '{s+=$1} END{print s+0}' || true)
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s${PER_SHARD}\n" \
            "$NOW" "$((NOW - START))" "$TOTAL" "$THR" "$FDS" "$TCP" "$CB" "$DB" "${FREE_MB:-0}"
        sleep "$SAMPLE_INTERVAL"
    done
} > "$RUNDIR/resources.tsv"

# Copy before stopping. The logs are flushed as the crawl runs, so this
# captures the run even if everything below goes wrong.
collect

# Normally there is nothing to stop: RUN_DURATION ends each crawl from inside
# and the supervisors exit on their own. This path covers the early exits, the
# disk guard and Ctrl-C.
#
# SIGTERM goes to the python process, not to the recorded pid. The recorded pid
# is the supervisor subshell and its child is `uv run`, which does not forward
# signals. That is why every earlier run was SIGKILLed without writing "Dumping
# Scrapy stats": the signal never reached the engine. Scrapy's own SIGTERM
# handler calls the same close path the timed shutdown uses.
echo "stopping     : $(date -Iseconds)"
STOP_START=$(date +%s)
if any_alive; then
    for pid in "${PIDS[@]}"; do
        for kid in $(descendants "$pid" 2>/dev/null); do
            [ "$kid" = "$pid" ] && continue
            kill -TERM "$kid" 2>/dev/null || true
        done
    done
fi
# The in-crawl shutdown cancels the parked requests, so what remains is bounded
# by download_timeout plus one per-domain gap. The elapsed time is printed: if
# a shard uses the whole window, something other than the timeout is holding it.
GRACE="${GRACE:-$SHUTDOWN_GRACE}"
for _ in $(seq 1 "$GRACE"); do
    any_alive || break
    sleep 1
done
echo "drained in   : $(($(date +%s) - STOP_START))s (grace ${GRACE}s)"
for pid in "${PIDS[@]}"; do
    for kid in $(descendants "$pid" 2>/dev/null | sed "1!G;h;$!d"); do
        kill -9 "$kid" 2>/dev/null || true
    done
    wait "$pid" 2>/dev/null || true
done

# Again, to pick up whatever the crawls flushed during shutdown.
collect

# Say plainly whether the run produced the artifacts the report needs. Every
# earlier run lost its stats dump and its Bloom filter without ever saying so,
# and the loss was only discovered later when the numbers were missing.
echo
echo "=== close check ==="
UNCLEAN=0
for i in $(seq 0 $((SHARDS - 1))); do
    if [ "$SHARDS" -gt 1 ]; then SFX="-${i}"; else SFX=""; fi
    STATS="data/stats${SFX}.json"
    # Not "Dumping Scrapy stats" from the log: that line is logged at INFO and
    # LOG_LEVEL is WARNING, so it is never present however cleanly the crawl
    # closed. finish_reason is written by the stats collector at close and is
    # the real marker. runstats.py writes this file every 30s, so its presence
    # alone proves nothing; the field inside it does.
    if grep -q '"finish_reason"' "$STATS" 2>/dev/null; then
        REASON=$(sed -n 's/.*"finish_reason": "\([^"]*\)".*/\1/p' "$STATS" | head -1)
        echo "shard $i     : closed cleanly (finish_reason=$REASON)"
    else
        echo "shard $i     : NO finish_reason in $STATS (unclean close)"
        UNCLEAN=1
    fi
    if [ -f "state/job-${i}/requests.bloom" ]; then
        echo "shard $i     : bloom filter persisted"
    else
        echo "shard $i     : NO BLOOM FILTER (unclean close)"
        UNCLEAN=1
    fi
    MISSING=$(sed -n 's/.*"dispatch\/missing_stamp": \([0-9]*\).*/\1/p' "$STATS" 2>/dev/null | head -1)
    if [ -n "$MISSING" ] && [ "$MISSING" != "0" ]; then
        echo "shard $i     : $MISSING response(s) had no dispatch stamp"
        UNCLEAN=1
    fi
    FOREIGN=$(sed -n 's/.*"handoff\/foreign_dropped": \([0-9]*\).*/\1/p' "$STATS" 2>/dev/null | head -1)
    if [ -n "$FOREIGN" ] && [ "$FOREIGN" != "0" ]; then
        echo "shard $i     : $FOREIGN foreign url(s) arrived in the handoff inbox"
    fi
done
if [ -s "$SUPERVISOR_LOG" ]; then
    echo "restarts     : see $SUPERVISOR_LOG"
    cat "$SUPERVISOR_LOG"
else
    echo "restarts     : none"
fi
for f in data/violation-trace*.log; do
    [ -e "$f" ] || continue
    if [ -s "$f" ]; then
        echo "POLITENESS   : $f is NOT empty, short gaps were recorded"
    fi
done
if [ "$UNCLEAN" = "1" ]; then
    echo "WARNING: at least one shard did not close cleanly"
fi

echo "finished     : $(date -Iseconds)"
echo "run dir      : $RUNDIR"
