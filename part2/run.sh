#!/bin/sh
# =============================================================================
#  CISC 5950 — Lab 2  |  Part 2  |  Task 2.1
#  run.sh — User Session Reconstruction (Secondary Sort)
# =============================================================================

HDFS="${HDFS:-/usr/local/hadoop/bin/hdfs}"
HADOOP="${HADOOP:-/usr/local/hadoop/bin/hadoop}"
STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar"

DATA_DIR="/mapreduce-test/map-reduce-assignment2/part2"
HDFS_INPUT="/ecommerce/raw"
HDFS_OUTPUT="/ecommerce/sessions"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/task2.1.log"

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

header "CISC 5950  Lab 2  ·  Task 2.1  —  User Session Reconstruction"

step "Starting Hadoop cluster"
../../start.sh 2>&1 || true
$HDFS dfsadmin -report > /dev/null 2>&1 || die "Cluster unreachable."
success "Hadoop cluster is up."

step "Uploading clickstream data to HDFS"
mkdir -p "${LOG_DIR}"
$HDFS dfs -mkdir -p "${HDFS_INPUT}"
info "Uploading clickstream_large.csv..."
$HDFS dfs -copyFromLocal -f \
    "${DATA_DIR}/clickstream_large.csv" \
    "${HDFS_INPUT}/" >> "${LOG_FILE}" 2>&1 \
    || die "Upload failed."
success "Data uploaded to ${HDFS_INPUT}/"

step "Preparing workspace"
$HDFS dfs -rm -r -f "${HDFS_OUTPUT}" >> "${LOG_FILE}" 2>&1
success "Workspace ready."

step "Launching MapReduce Job  [Task 2.1 — Secondary Sort]"
info "Pattern      : Full Secondary Sort"
info "Composite Key: user_id + timestamp"
info "Partitioner  : KeyFieldBasedPartitioner (partition by user_id only)"
info "Comparator   : KeyFieldBasedComparator  (sort by user_id + timestamp)"
info "Reducers     : 2"
printf "\n"

$HADOOP jar "$STREAMING_JAR" \
    -D mapreduce.job.name="CISC5950-Lab2-Task2.1-SecondarySort" \
    -D mapreduce.job.reduces=2 \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.map.output.key.field.separator="\t" \
    -D mapreduce.partition.keypartitioner.options="-k1,1" \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,1 -k2,2" \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -file  mapper.py  \
    -mapper  "python3 mapper.py" \
    -file  reducer.py  \
    -reducer "python3 reducer.py" \
    -input  "${HDFS_INPUT}/clickstream_large.csv" \
    -output "${HDFS_OUTPUT}" \
    2>&1 | tee -a "${LOG_FILE}"

$HDFS dfs -test -e "${HDFS_OUTPUT}/_SUCCESS" \
    || die "Job failed. Check ${LOG_FILE}"
success "MapReduce job completed successfully."

step "Verifying Secondary Sort"
info "Checking data distribution across reducers..."
COUNT0=$($HDFS dfs -cat "${HDFS_OUTPUT}/part-00000" 2>/dev/null | grep -c "^SESS_" || echo 0)
COUNT1=$($HDFS dfs -cat "${HDFS_OUTPUT}/part-00001" 2>/dev/null | grep -c "^SESS_" || echo 0)
info "Reducer 0 sessions: ${COUNT0}"
info "Reducer 1 sessions: ${COUNT1}"

step "Results"
printf "\n"
$HDFS dfs -cat "${HDFS_OUTPUT}/part-"* 2>/dev/null \
    | grep -A 200 "TASK 2.1" \
    | tee "${LOG_DIR}/task2.1_results.txt"

printf "\n"
info "Logs    : ${LOG_FILE}"
info "Results : ${LOG_DIR}/task2.1_results.txt"
info "HDFS    : ${HDFS_OUTPUT}/"

step "Stopping Hadoop cluster"
../../stop.sh 2>&1 || true
success "Cluster stopped."

header "Task 2.1 Complete"
