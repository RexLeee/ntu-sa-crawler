#!/usr/bin/env bash
# Long-duration run with resource sampling.
#
# Short samples (5-11 min) are all still in ramp-up: the domain frontier is
# still growing when they end, so their pages/sec cannot be extrapolated.
# This samples resources on a fixed interval so the growth curve can be
# plotted, not just its endpoint.
#
# Usage: ops/run_hour.sh [duration_seconds] [seeds_file]
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"

# The soft limit is 1024. A measured run peaked at 2,021 open descriptors,
# most of them robots.txt fetches and DNS sockets rather than page transfers.
# The hard limit is 1048576, so this needs no root and no config file.
ulimit -n 65536

DURATION="${1:-3600}"
SEEDS="${2:-seeds/seeds_1000.txt}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-30}"
STAMP=$(date +%Y%m%d-%H%M%S)
RUNDIR="data/run-${STAMP}"
mkdir -p "$RUNDIR"

# Start clean so the metrics describe this run alone.
rm -f data/crawled.log.gz data/discovered.log.gz
rm -rf state/job

echo "run dir      : $RUNDIR"
echo "duration     : ${DURATION}s"
echo "seeds        : $SEEDS ($(grep -c '^https' "$SEEDS" 2>/dev/null || echo 0) urls)"
echo "fd limit     : $(ulimit -n)"
echo "started      : $(date -Iseconds)"

# No CLOSESPIDER_TIMEOUT. With a large CONCURRENT_REQUESTS its graceful
# shutdown waits for every in-flight request to time out, which took over five
# minutes in testing and had to be killed anyway. The logs are flushed as the
# crawl runs, so stopping the process directly loses nothing but the final
# gzip end-of-stream marker, which the analysis tools already tolerate.
uv run scrapy crawl broad -a "seeds=${SEEDS}" --logfile "$RUNDIR/scrapy.log" &
CRAWL_PID=$!
echo "pid          : $CRAWL_PID"

# Copy the logs into the run directory whatever happens, including Ctrl-C or
# the machine being shut down. A previous run lost its final statistics
# because this only ran on the clean-exit path.
collect() {
    cp data/crawled.log.gz data/discovered.log.gz "$RUNDIR/" 2>/dev/null || true
}
trap collect EXIT INT TERM

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
        if [ $((NOW - START)) -ge "$DURATION" ]; then
            break
        fi
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

# Copy before stopping. The logs are flushed as the crawl runs, so this
# captures the run even if everything below goes wrong.
collect

# SIGTERM first so the dupefilter gets its chance to persist, then SIGKILL,
# because graceful shutdown does not finish at this concurrency: it waits for
# every parked request to time out, which took over five minutes in testing.
echo "stopping     : $(date -Iseconds)"
kill -TERM "$CRAWL_PID" 2>/dev/null || true
for _ in $(seq 1 20); do
    kill -0 "$CRAWL_PID" 2>/dev/null || break
    sleep 1
done
# `uv run` is the parent; killing it alone leaves the python child running.
pkill -9 -P "$CRAWL_PID" 2>/dev/null || true
kill -9 "$CRAWL_PID" 2>/dev/null || true
wait "$CRAWL_PID" 2>/dev/null || true

# Again, to pick up whatever the crawl flushed during shutdown.
collect
echo "finished     : $(date -Iseconds)"
echo "run dir      : $RUNDIR"
