#!/bin/sh
# =============================================================================
#  CISC 5950 — Lab 2  |  Part 1A  |  Task 1.1
#  run.sh  —  Data Quality Assessment & Cleaning
#  NYC Parking Violations  ·  MapReduce Pipeline
# =============================================================================

# ── Hadoop paths ──────────────────────────────────────────────────────────────
HDFS="${HDFS:-/usr/local/hadoop/bin/hdfs}"
HADOOP="${HADOOP:-/usr/local/hadoop/bin/hadoop}"
STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar"

HDFS_INPUT="/parking/raw/parking_raw.csv"
HDFS_OUTPUT="/parking/clean"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/task1.1.log"

# ── Helpers ───────────────────────────────────────────────────────────────────
HR="────────────────────────────────────────────────────────────"
info()    { printf "  \033[1;34m[INFO]\033[0m  %s\n"    "$*"; }
success() { printf "  \033[1;32m[OK]\033[0m    %s\n"    "$*"; }
warn()    { printf "  \033[1;33m[WARN]\033[0m  %s\n"    "$*"; }
error()   { printf "  \033[1;31m[ERROR]\033[0m %s\n"    "$*" >&2; }
step()    { printf "\n\033[1;37m── %s\033[0m\n" "$*"; }
header()  {
    printf "\n\033[1;36m┌%s┐\033[0m\n" "$HR"
    printf "\033[1;36m│  %-56s  │\033[0m\n" "$*"
    printf "\033[1;36m└%s┘\033[0m\n\n" "$HR"
}

die() { error "$*"; exit 1; }

# =============================================================================
header "CISC 5950  Lab 2  ·  Task 1.1  —  Data Quality Cleaning"

# ── Start cluster ─────────────────────────────────────────────────────────────
step "Starting Hadoop cluster"
../../start.sh 2>&1 || true   # non-zero is OK when processes are already running
# Verify cluster is actually reachable before proceeding
$HDFS dfsadmin -report > /dev/null 2>&1 \
    || die "Hadoop cluster unreachable — check namenode/datanode status."
success "Hadoop cluster is up."

# ── Prepare workspace ─────────────────────────────────────────────────────────
step "Preparing HDFS workspace"
mkdir -p "${LOG_DIR}"

info "Removing previous output directory..."
$HDFS dfs -rm -r -f "${HDFS_OUTPUT}" >> "${LOG_FILE}" 2>&1

info "Creating HDFS input directory..."
$HDFS dfs -mkdir -p /parking/raw >> "${LOG_FILE}" 2>&1

info "Uploading parking violations CSV to HDFS..."
$HDFS dfs -copyFromLocal -f parking_raw.csv /parking/raw/ \
    >> "${LOG_FILE}" 2>&1 \
    || die "Failed to upload data. See ${LOG_FILE}"
success "Data uploaded to ${HDFS_INPUT}"

# ── Run MapReduce job ─────────────────────────────────────────────────────────
step "Launching MapReduce Job  [Task 1.1 — Data Cleaning]"
info "Mapper  : mapper.py"
info "Reducer : reducer.py"
info "Input   : ${HDFS_INPUT}"
info "Output  : ${HDFS_OUTPUT}/"
printf "\n"

$HADOOP jar "$STREAMING_JAR" \
    -D mapreduce.job.name="CISC5950-Lab2-Task1.1-DataCleaning" \
    -D mapreduce.job.reduces=1 \
    -file  mapper.py  \
    -mapper  mapper.py  \
    -file  reducer.py  \
    -reducer reducer.py \
    -input  "${HDFS_INPUT}"  \
    -output "${HDFS_OUTPUT}" \
    2>&1 | tee -a "${LOG_FILE}"

# ── Verify success ────────────────────────────────────────────────────────────
$HDFS dfs -test -e "${HDFS_OUTPUT}/_SUCCESS" \
    || die "MapReduce job did NOT produce a _SUCCESS marker. Check ${LOG_FILE}"

success "MapReduce job completed successfully."

# ── Results ───────────────────────────────────────────────────────────────────
step "Results"

# Record count (subtract 1 for header, ignore report lines)
RECORD_COUNT=$(
    $HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
    | grep -v '^[│┌└├─┐┘]' \
    | grep -v '^\s*$' \
    | tail -n +2 \
    | wc -l
)
printf "  %-30s \033[1;32m%s records\033[0m\n" "Cleaned output rows:" "${RECORD_COUNT}"

# Sample rows
printf "\n  \033[1;37mSample — first 3 data rows:\033[0m\n"
printf "  %s\n" "${HR}"
$HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
    | grep -v '^[│┌└├─┐┘]' \
    | grep -v '^\s*$' \
    | head -4 \
    | while IFS= read -r row; do
        printf "  %s\n" "$row"
      done
printf "  %s\n" "${HR}"

# Full quality report (printed by reducer)
printf "\n  \033[1;37mData Quality Report:\033[0m\n"
$HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
    | grep '^[│┌└├─┐┘]' \
    | sed 's/^/  /'

printf "\n"
info "Full logs available at: ${LOG_FILE}"
info "Cleaned dataset at    : ${HDFS_OUTPUT}/"

# ── Stop cluster ──────────────────────────────────────────────────────────────
step "Stopping Hadoop cluster"
../../stop.sh 2>&1 || true
success "Cluster stopped."

header "Task 1.1 Complete"
