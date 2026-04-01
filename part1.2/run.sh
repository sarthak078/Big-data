#!/bin/sh
# =============================================================================
#  CISC 5950 — Lab 2  |  Part 1B  |  Task 1.2
#  run.sh  —  Peak Enforcement Time Analysis
#  NYC Parking Violations  ·  MapReduce Pipeline
# =============================================================================

HDFS="${HDFS:-/usr/local/hadoop/bin/hdfs}"
HADOOP="${HADOOP:-/usr/local/hadoop/bin/hadoop}"
STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar"

HDFS_INPUT="/parking/clean"
HDFS_OUTPUT="/parking/analysis/peak_hours"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/task1.2.log"

# ── Helpers ───────────────────────────────────────────────────────────────────
info()    { printf "  \033[1;34m[INFO]\033[0m  %s\n"  "$*"; }
success() { printf "  \033[1;32m[OK]\033[0m    %s\n"  "$*"; }
error()   { printf "  \033[1;31m[ERROR]\033[0m %s\n"  "$*" >&2; }
step()    { printf "\n\033[1;37m── %s\033[0m\n" "$*"; }
header()  {
    HR="────────────────────────────────────────────────────────────"
    printf "\n\033[1;36m┌%s┐\033[0m\n" "$HR"
    printf "\033[1;36m│  %-56s  │\033[0m\n" "$*"
    printf "\033[1;36m└%s┘\033[0m\n\n" "$HR"
}
die() { error "$*"; exit 1; }

# =============================================================================
header "CISC 5950  Lab 2  ·  Task 1.2  —  Peak Enforcement"

# ── Start cluster ─────────────────────────────────────────────────────────────
step "Starting Hadoop cluster"
../../start.sh 2>&1 || true
$HDFS dfsadmin -report > /dev/null 2>&1 \
    || die "Hadoop cluster unreachable."
success "Hadoop cluster is up."

# ── Verify input ──────────────────────────────────────────────────────────────
step "Verifying input"
$HDFS dfs -test -e "${HDFS_INPUT}/_SUCCESS" \
    || die "Cleaned data not found at ${HDFS_INPUT}. Run Task 1.1 first."
success "Input verified at ${HDFS_INPUT}"

# ── Prepare workspace ─────────────────────────────────────────────────────────
step "Preparing workspace"
mkdir -p "${LOG_DIR}"
$HDFS dfs -rm -r -f "${HDFS_OUTPUT}" >> "${LOG_FILE}" 2>&1
success "Workspace ready."

# ── Run MapReduce ─────────────────────────────────────────────────────────────
step "Launching MapReduce Job  [Task 1.2 — Hourly Violation Counts]"
info "Mapper  : mapper.py   (emit hour → 1)"
info "Reducer : reducer.py  (aggregate + rank + recommend)"
info "Input   : ${HDFS_INPUT}/"
info "Output  : ${HDFS_OUTPUT}/"
printf "\n"

$HADOOP jar "$STREAMING_JAR" \
    -D mapreduce.job.name="CISC5950-Lab2-Task1.2-PeakEnforcement" \
    -D mapreduce.job.reduces=1 \
    -file  mapper.py  \
    -mapper  mapper.py  \
    -file  reducer.py  \
    -reducer reducer.py \
    -input  "${HDFS_INPUT}" \
    -output "${HDFS_OUTPUT}" \
    2>&1 | tee -a "${LOG_FILE}"

$HDFS dfs -test -e "${HDFS_OUTPUT}/_SUCCESS" \
    || die "MapReduce job failed. Check ${LOG_FILE}"
success "MapReduce job completed successfully."

# ── Display results ───────────────────────────────────────────────────────────
step "Results"
printf "\n"
$HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null | tee "${LOG_DIR}/task1.2_results.txt"

printf "\n"
info "Full logs    : ${LOG_FILE}"
info "Results file : ${LOG_DIR}/task1.2_results.txt"
info "HDFS output  : ${HDFS_OUTPUT}/"

# ── Stop cluster ──────────────────────────────────────────────────────────────
step "Stopping Hadoop cluster"
../../stop.sh 2>&1 || true
success "Cluster stopped."

header "Task 1.2 Complete"
