#!/bin/sh
# =============================================================================
#  CISC 5950 — Lab 2  |  Part 1A  |  Task 1.1
#  run.sh  —  Data Quality Assessment & Cleaning
#  NYC Parking Violations  ·  MapReduce Pipeline
# =============================================================================

HDFS="${HDFS:-/usr/local/hadoop/bin/hdfs}"
HADOOP="${HADOOP:-/usr/local/hadoop/bin/hadoop}"
STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar"

HDFS_INPUT="/parking/raw/parking_raw.csv"
HDFS_OUTPUT="/parking/clean"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/task1.1.log"
TMP_DIR="/home/hadoop-tmp"

HR="────────────────────────────────────────────────────────────"
info()    { printf "  \033[1;34m[INFO]\033[0m  %s\n"    "$*"; }
success() { printf "  \033[1;32m[OK]\033[0m    %s\n"    "$*"; }
error()   { printf "  \033[1;31m[ERROR]\033[0m %s\n"    "$*" >&2; }
step()    { printf "\n\033[1;37m── %s\033[0m\n" "$*"; }
header()  {
    printf "\n\033[1;36m┌%s┐\033[0m\n" "$HR"
    printf "\033[1;36m│  %-56s  │\033[0m\n" "$*"
    printf "\033[1;36m└%s┘\033[0m\n\n" "$HR"
}
die() { error "$*"; exit 1; }

header "CISC 5950  Lab 2  ·  Task 1.1  —  Data Quality Cleaning"

# ── Start cluster ─────────────────────────────────────────────────────────────
step "Starting Hadoop cluster"
../../start.sh 2>&1 || true
$HDFS dfsadmin -report > /dev/null 2>&1 \
    || die "Hadoop cluster unreachable."
success "Hadoop cluster is up."

# ── Check if already done ─────────────────────────────────────────────────────
step "Checking existing output"
if $HDFS dfs -test -e "${HDFS_OUTPUT}/_SUCCESS" 2>/dev/null; then
    success "Cleaned data already exists at ${HDFS_OUTPUT}/"
    info "Skipping MapReduce job — data already cleaned."

    # Show existing results
    step "Results (from previous run)"
    RECORD_COUNT=$(
        $HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
        | grep -v '^[│┌└├─┐┘]' \
        | grep -v '^\s*$' \
        | tail -n +2 \
        | wc -l
    )
    printf "  %-30s \033[1;32m%s records\033[0m\n" "Cleaned output rows:" "${RECORD_COUNT}"
    printf "\n  \033[1;37mData Quality Report:\033[0m\n"
    $HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
        | grep '^[│┌└├─┐┘]' | sed 's/^/  /'
    printf "\n"
    info "Cleaned dataset at: ${HDFS_OUTPUT}/"

    step "Stopping Hadoop cluster"
    ../../stop.sh 2>&1 || true
    success "Cluster stopped."
    header "Task 1.1 Complete"
    exit 0
fi
info "No existing output found — proceeding with MapReduce job."

# ── Free up /tmp disk space ───────────────────────────────────────────────────
step "Freeing disk space"
info "Cleaning /tmp Hadoop scratch files..."
rm -rf /tmp/hadoop-root/mapred/local/localRunner/ 2>/dev/null || true
rm -rf /tmp/hadoop-* 2>/dev/null || true
info "Available space after cleanup:"
df -h /tmp | tail -1 | awk '{printf "    /tmp: %s used of %s (%s free)\n", $3, $2, $4}'

# Create dedicated temp dir on /home (larger partition)
mkdir -p "${TMP_DIR}"
info "Using temp dir: ${TMP_DIR}"
df -h /home | tail -1 | awk '{printf "    /home: %s used of %s (%s free)\n", $3, $2, $4}'

# ── Prepare workspace ─────────────────────────────────────────────────────────
step "Preparing HDFS workspace"
mkdir -p "${LOG_DIR}"

info "Removing previous output directory..."
$HDFS dfs -rm -r -f "${HDFS_OUTPUT}" >> "${LOG_FILE}" 2>&1

info "Creating HDFS input directory..."
$HDFS dfs -mkdir -p /parking/raw >> "${LOG_FILE}" 2>&1

info "Uploading parking violations CSV to HDFS..."
$HDFS dfs -test -e "${HDFS_INPUT}" 2>/dev/null || \
    $HDFS dfs -copyFromLocal -f parking_raw.csv /parking/raw/ \
        >> "${LOG_FILE}" 2>&1 \
        || die "Failed to upload data. See ${LOG_FILE}"
success "Data ready at ${HDFS_INPUT}"

# ── Run MapReduce job ─────────────────────────────────────────────────────────
step "Launching MapReduce Job  [Task 1.1 — Data Cleaning]"
info "Mapper  : mapper.py"
info "Reducer : reducer.py"
info "Temp dir: ${TMP_DIR} (avoids /tmp disk-full errors)"
info "Input   : ${HDFS_INPUT}"
info "Output  : ${HDFS_OUTPUT}/"
printf "\n"

$HADOOP jar "$STREAMING_JAR" \
    -D mapreduce.job.name="CISC5950-Lab2-Task1.1-DataCleaning" \
    -D mapreduce.job.reduces=1 \
    -D mapred.local.dir="${TMP_DIR}" \
    -D mapreduce.cluster.local.dir="${TMP_DIR}" \
    -D mapreduce.task.io.sort.mb=200 \
    -file  mapper.py  \
    -mapper  mapper.py  \
    -file  reducer.py  \
    -reducer reducer.py \
    -input  "${HDFS_INPUT}"  \
    -output "${HDFS_OUTPUT}" \
    2>&1 | tee -a "${LOG_FILE}"

# ── Verify success ────────────────────────────────────────────────────────────
$HDFS dfs -test -e "${HDFS_OUTPUT}/_SUCCESS" \
    || die "Job failed. Check ${LOG_FILE}"
success "MapReduce job completed successfully."

# ── Results ───────────────────────────────────────────────────────────────────
step "Results"
RECORD_COUNT=$(
    $HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
    | grep -v '^[│┌└├─┐┘]' \
    | grep -v '^\s*$' \
    | tail -n +2 \
    | wc -l
)
printf "  %-30s \033[1;32m%s records\033[0m\n" "Cleaned output rows:" "${RECORD_COUNT}"

printf "\n  \033[1;37mSample — first 3 data rows:\033[0m\n"
printf "  %s\n" "${HR}"
$HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
    | grep -v '^[│┌└├─┐┘]' \
    | grep -v '^\s*$' \
    | head -4 \
    | while IFS= read -r row; do printf "  %s\n" "$row"; done
printf "  %s\n" "${HR}"

printf "\n  \033[1;37mData Quality Report:\033[0m\n"
$HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
    | grep '^[│┌└├─┐┘]' | sed 's/^/  /'

printf "\n"
info "Full logs at      : ${LOG_FILE}"
info "Cleaned dataset at: ${HDFS_OUTPUT}/"

# ── Stop cluster ──────────────────────────────────────────────────────────────
step "Stopping Hadoop cluster"
../../stop.sh 2>&1 || true
success "Cluster stopped."

header "Task 1.1 Complete"
