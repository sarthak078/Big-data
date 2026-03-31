#!/bin/sh
# =============================================================================
#  CISC 5950 — Lab 2  |  Part 2  |  Task 2.2
#  run.sh — K-Means Customer Segmentation (Iterative MapReduce)
# =============================================================================

HDFS="${HDFS:-/usr/local/hadoop/bin/hdfs}"
HADOOP="${HADOOP:-/usr/local/hadoop/bin/hadoop}"
STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar"
DATA_DIR="/mapreduce-test/map-reduce-assignment2/part2"
HDFS_INPUT="/ecommerce/raw/clickstream_large.csv"
HDFS_RFM="/ecommerce/rfm"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/task2.2.log"
K=4
MAX_ITER=20
CONVERGENCE=0.001

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

header "CISC 5950  Lab 2  Task 2.2  K-Means Customer Segmentation"

step "Starting Hadoop cluster"
../../start.sh 2>&1 || true
$HDFS dfsadmin -report > /dev/null 2>&1 || die "Cluster unreachable."
success "Hadoop cluster is up."

step "Preparing data"
mkdir -p "${LOG_DIR}"
$HDFS dfs -mkdir -p /ecommerce/raw/
$HDFS dfs -test -e "${HDFS_INPUT}" || {
    info "Uploading clickstream_large.csv..."
    $HDFS dfs -copyFromLocal -f "${DATA_DIR}/clickstream_large.csv" \
        /ecommerce/raw/ >> "${LOG_FILE}" 2>&1 || die "Upload failed."
}
success "Data ready."

# STEP 1: RFM Features
step "Step 1: Computing RFM Features"
$HDFS dfs -rm -r -f "${HDFS_RFM}" >> "${LOG_FILE}" 2>&1

$HADOOP jar "$STREAMING_JAR" \
    -D mapreduce.job.name="Task2.2-RFM" \
    -D mapreduce.job.reduces=1 \
    -file  rfm_mapper.py \
    -mapper  rfm_mapper.py \
    -file  rfm_reducer.py \
    -reducer rfm_reducer.py \
    -input  "${HDFS_INPUT}" \
    -output "${HDFS_RFM}" \
    2>&1 | tee -a "${LOG_FILE}"

$HDFS dfs -test -e "${HDFS_RFM}/_SUCCESS" || die "RFM job failed."
success "RFM features computed."

TOTAL_USERS=$($HDFS dfs -cat "${HDFS_RFM}/part-"* 2>/dev/null | wc -l)
info "Total unique users: ${TOTAL_USERS}"

# Download RFM data locally
$HDFS dfs -cat "${HDFS_RFM}/part-"* 2>/dev/null > rfm_data.txt

# STEP 2: Init Centroids
step "Step 2: Initializing ${K} Random Centroids"
shuf -n ${K} rfm_data.txt \
    | awk -F'\t' 'BEGIN{c=0}{print c"\t"$2"\t"$3"\t"$4"\t1"; c++}' \
    > centroids.txt
info "Initial centroids set."
cat centroids.txt

# STEP 3: Iterate
step "Step 3: K-Means Iterations"
iteration=0
converged=0

while [ $iteration -lt $MAX_ITER ]; do
    iteration=$((iteration + 1))
    printf "  Iteration %d: " "$iteration"

    cat rfm_data.txt \
        | python3 kmeans_mapper.py \
        | sort -k1,1n \
        | python3 kmeans_reducer.py \
        > new_centroids.txt

    if [ ! -s new_centroids.txt ]; then
        error "Empty centroids at iteration ${iteration}"
        break
    fi

    # Check max movement
    MAX_MOVEMENT=$(python3 - << 'PYEOF'
import math
old = {}
with open('centroids.txt') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) >= 4:
            old[int(parts[0])] = (float(parts[1]), float(parts[2]), float(parts[3]))
new = {}
with open('new_centroids.txt') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) >= 4:
            new[int(parts[0])] = (float(parts[1]), float(parts[2]), float(parts[3]))
max_move = 0.0
for k in old:
    if k in new:
        dR = old[k][0]-new[k][0]
        dF = old[k][1]-new[k][1]
        dM = old[k][2]-new[k][2]
        d = math.sqrt(dR**2+dF**2+dM**2)
        if d > max_move:
            max_move = d
print(f"{max_move:.8f}")
PYEOF
)

    printf "max movement = %s\n" "${MAX_MOVEMENT}"
    cp new_centroids.txt centroids.txt

    DONE=$(python3 -c "print(1 if float('${MAX_MOVEMENT}') < ${CONVERGENCE} else 0)" 2>/dev/null)
    if [ "$DONE" = "1" ]; then
        converged=1
        break
    fi
done

if [ "$converged" = "1" ]; then
    success "Converged after ${iteration} iterations!"
else
    info "Reached max ${MAX_ITER} iterations."
fi

# STEP 4: Final Analysis
step "Step 4: Final Cluster Analysis"

cat rfm_data.txt | python3 kmeans_mapper.py | sort -k1,1n > final_assignments.txt

python3 analysis.py | tee "${LOG_DIR}/task2.2_results.txt"

printf "\n"
info "Iterations  : ${iteration}"
info "Converged   : ${converged}"
info "Logs        : ${LOG_FILE}"
info "Results     : ${LOG_DIR}/task2.2_results.txt"

step "Stopping Hadoop cluster"
../../stop.sh 2>&1 || true
success "Cluster stopped."

header "Task 2.2 Complete"
