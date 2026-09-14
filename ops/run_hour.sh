#!/usr/bin/env bash
# Long-duration baseline run.
#
# Short samples (5-11 min) are all still in ramp-up: the domain frontier was
# still growing when they ended, so their pages/sec cannot be extrapolated.
# This samples resources on a fixed interval so the growth curve can be
# plotted, not just its endpoint.
#
# Usage: ops/run_hour.sh [duration_seconds]
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"

# The soft limit is 1024. CONCURRENT_REQUESTS=200 plus DNS sockets and the
# gzip log handles approaches that, and raising concurrency would hit
# "too many open files" before memory or bandwidth. The hard limit is
# 1048576, so this needs no root and no config file.
ulimit -n 65536

DURATION="${1:-3600}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-30}"
STAMP=$(date +%Y%m%d-%H%M%S)
RUNDIR="data/run-${STAMP}"
mkdir -p "$RUNDIR"

# Start clean so the metrics describe this run alone.
rm -f data/crawled.log.gz data/discovered.log.gz

echo "run dir      : $RUNDIR"
echo "duration     : ${DURATION}s"
echo "fd limit     : $(ulimit -n)"
echo "started      : $(date -Iseconds)"

uv run scrapy crawl broad \
    -s "CLOSESPIDER_TIMEOUT=${DURATION}" \
    --logfile "$RUNDIR/scrapy.log" &
CRAWL_PID=$!
echo "pid          : $CRAWL_PID"

# The crawl runs under `uv run`, so the python process is a child of the
# recorded pid. Sample the whole tree rather than the launcher.
sample_tree_rss() {
    local total=0 pid
    for pid in $(pgrep -P "$CRAWL_PID" 2>/dev/null) "$CRAWL_PID"; do
        local rss
        rss=$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || echo 0)
        total=$((total + ${rss:-0}))
    done
    echo "$total"
}

leaf_pid() {
    local child
    child=$(pgrep -P "$CRAWL_PID" 2>/dev/null | head -1)
    echo "${child:-$CRAWL_PID}"
}

{
    printf 'ts\telapsed\trss_kb\tthreads\tfds\ttcp_est\tcrawled_bytes\tdiscovered_bytes\n'
    START=$(date +%s)
    while kill -0 "$CRAWL_PID" 2>/dev/null; do
        NOW=$(date +%s)
        LEAF=$(leaf_pid)
        RSS=$(sample_tree_rss)
        THR=$(awk '/^Threads:/{print $2}' "/proc/$LEAF/status" 2>/dev/null || echo 0)
        FDS=$(ls "/proc/$LEAF/fd" 2>/dev/null | wc -l)
        TCP=$(ss -tn state established 2>/dev/null | tail -n +2 | wc -l)
        CB=$(stat -c %s data/crawled.log.gz 2>/dev/null || echo 0)
        DB=$(stat -c %s data/discovered.log.gz 2>/dev/null || echo 0)
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$NOW" "$((NOW - START))" "$RSS" "$THR" "$FDS" "$TCP" "$CB" "$DB"
        sleep "$SAMPLE_INTERVAL"
    done
} > "$RUNDIR/resources.tsv"

wait "$CRAWL_PID" || true

cp data/crawled.log.gz data/discovered.log.gz "$RUNDIR/" 2>/dev/null || true
echo "finished     : $(date -Iseconds)"
echo "run dir      : $RUNDIR"
