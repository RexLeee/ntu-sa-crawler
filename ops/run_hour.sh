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
# Usage: ops/run_hour.sh [duration_seconds] [seeds_file]
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"

# The soft limit is 1024. Each (domain, priority) pair in the frontier is a
# FifoDiskQueue holding two open files, head and tail, so descriptors track
# the frontier's shape rather than the number of live connections: a measured
# run reached 7,569 queues and 15,422 descriptors. The hard limit is
# 1,048,576, so raising this needs no root and no config file.
ulimit -n 1000000

# glibc raises its mmap threshold dynamically as a program frees large blocks,
# after which page bodies of a few hundred KB are served from the heap and
# fragment it instead of being returned to the OS. Pinning the threshold and
# capping arenas keeps RSS closer to the live set. data/objects.log shows
# whether it worked: flat object counts against rising RSS mean fragmentation.
export MALLOC_ARENA_MAX=2
export MALLOC_MMAP_THRESHOLD_=131072

DURATION="${1:-3600}"
SEEDS="${2:-seeds/seeds_1000.txt}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-30}"
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

# Start clean so the metrics describe this run alone.
rm -f data/crawled*.log.gz data/discovered*.log.gz data/runstats*.tsv data/objects*.log
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
if ls -d state/job state/job-* >/dev/null 2>&1; then
    echo "could not clear the frontier; something still holds it:" >&2
    ls -d state/job state/job-* >&2
    exit 1
fi

echo "run dir      : $RUNDIR"
echo "duration     : ${DURATION}s"
echo "shards       : $SHARDS"
echo "seeds        : $SEEDS ($(grep -c '^https' "$SEEDS" 2>/dev/null || echo 0) urls)"
echo "fd limit     : $(ulimit -n)"
echo "started      : $(date -Iseconds)"

# No CLOSESPIDER_TIMEOUT. With a large CONCURRENT_REQUESTS its graceful
# shutdown waits for every in-flight request to time out, which took over five
# minutes in testing and had to be killed anyway. The logs are flushed as the
# crawl runs, so stopping the process directly loses nothing but the final
# gzip end-of-stream marker, which the analysis tools already tolerate.
PIDS=()
for i in $(seq 0 $((SHARDS - 1))); do
    uv run scrapy crawl broad \
        -a "seeds=${SEEDS}" -a "shard=${i}" -a "shards=${SHARDS}" \
        -s "JOBDIR=state/job-${i}" \
        --logfile "$RUNDIR/scrapy-${i}.log" &
    PIDS+=($!)
    echo "shard $i pid : ${PIDS[-1]}"
done

# Copy the logs into the run directory whatever happens, including Ctrl-C or
# the machine being shut down. A previous run lost its final statistics
# because this only ran on the clean-exit path.
collect() {
    cp data/crawled*.log.gz data/discovered*.log.gz "$RUNDIR/" 2>/dev/null || true
    cp data/runstats*.tsv data/objects*.log "$RUNDIR/" 2>/dev/null || true
    cp data/violation-trace*.log "$RUNDIR/" 2>/dev/null || true
}

# The workers must not outlive this script. A supervisor that dies on an
# unexpected error used to leave four crawls running, which then held their
# JOBDIR open and corrupted the next run's frontier. `uv run` is the parent,
# so its child has to be killed too.
reap() {
    local pid
    for pid in "${PIDS[@]:-}"; do
        pkill -9 -P "$pid" 2>/dev/null || true
        kill -9 "$pid" 2>/dev/null || true
    done
}
trap 'collect; reap' EXIT INT TERM

# Each crawl runs under `uv run`, so the python process is a child of the
# recorded pid. Sample the whole tree rather than the launcher.
tree_rss() {
    local root="$1" total=0 pid rss
    for pid in $(pgrep -P "$root" 2>/dev/null) "$root"; do
        rss=$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
        total=$((total + ${rss:-0}))
    done
    echo "$total"
}

leaf_pid() {
    local child
    child=$(pgrep -P "$1" 2>/dev/null | head -1)
    echo "${child:-$1}"
}

any_alive() {
    local pid
    for pid in "${PIDS[@]}"; do
        kill -0 "$pid" 2>/dev/null && return 0
    done
    return 1
}

{
    # One rss column per shard, so a shard that grows faster than the others
    # is visible rather than hidden in the total. That is what decides how
    # many shards this machine can hold.
    header='ts\telapsed\trss_kb\tthreads\tfds\ttcp_est\tcrawled_bytes\tdiscovered_bytes'
    for i in $(seq 0 $((SHARDS - 1))); do header="${header}\trss_kb_${i}"; done
    printf "${header}\n"
    START=$(date +%s)
    while any_alive; do
        NOW=$(date +%s)
        if [ $((NOW - START)) -ge "$DURATION" ]; then
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
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s${PER_SHARD}\n" \
            "$NOW" "$((NOW - START))" "$TOTAL" "$THR" "$FDS" "$TCP" "$CB" "$DB"
        sleep "$SAMPLE_INTERVAL"
    done
} > "$RUNDIR/resources.tsv"

# Copy before stopping. The logs are flushed as the crawl runs, so this
# captures the run even if everything below goes wrong.
collect

# SIGTERM first so each dupefilter gets its chance to persist, then SIGKILL,
# because graceful shutdown does not finish at this concurrency: it waits for
# every parked request to time out, which took over five minutes in testing.
echo "stopping     : $(date -Iseconds)"
for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
done
for _ in $(seq 1 20); do
    any_alive || break
    sleep 1
done
for pid in "${PIDS[@]}"; do
    # `uv run` is the parent; killing it alone leaves the python child running.
    pkill -9 -P "$pid" 2>/dev/null || true
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
done

# Again, to pick up whatever the crawls flushed during shutdown.
collect
echo "finished     : $(date -Iseconds)"
echo "run dir      : $RUNDIR"
