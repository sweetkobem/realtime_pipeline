#!/bin/bash

FLINK_BIN="/opt/flink/bin/flink"
JAR_DIR="/opt/flink/scala"
SAVEPOINT_DIR="file:///opt/flink/savepoints"
SAVEPOINT_LOCAL_DIR="/opt/flink/savepoints"  # local version
JOBMAP_FILE="/opt/flink/jobmap.txt"

set -a
source /opt/flink/.env
set +a

function list_jobs() {
  echo "Listing running Flink jobs..."
  $FLINK_BIN list -r | grep -oE '\b[0-9a-f]{32}\b' || echo "No running jobs."
}

function savepoint_all() {
  echo "Triggering savepoints for all running jobs..."

  if [ -z "$SAVEPOINT_DIR" ]; then
    echo "Error: SAVEPOINT_DIR is not set."
    return 1
  fi

  if [ ! -d "$JAR_DIR" ]; then
    echo "Error: JAR_DIR '$JAR_DIR' does not exist or is not a directory."
    return 1
  fi

  mapfile -t jarfiles < <(find "$JAR_DIR" -type f -name "*.jar")

  if [ ${#jarfiles[@]} -eq 0 ]; then
    echo "⚠️ No JAR files found in $JAR_DIR (recursively). Skipping savepoint matching."
  fi

  > "$JOBMAP_FILE"

  mapfile -t running_jobs < <($FLINK_BIN list -r | grep 'RUNNING' | sed -E 's/^.* : ([0-9a-f]+) : (.*)$/\1|\2/')

  if [ ${#running_jobs[@]} -eq 0 ]; then
    echo "No jobs to savepoint."
    return
  fi

  for line in "${running_jobs[@]}"; do
    jobid=$(echo "$line" | cut -d'|' -f1)
    jobname=$(echo "$line" | cut -d'|' -f2)
    jobname=$(echo "$jobname" | sed -E 's/ *\([A-Z]+\)//')  # Remove (RUNNING)

    echo "Detected job: ID=$jobid | Name=$jobname"

    matched_jar=""
    matched_norm_base=""
    norm_jobname=$(echo "$jobname" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/ /g')

    for jarfile in "${jarfiles[@]}"; do
      jarname=$(basename "$jarfile")
      jarbase="${jarname%.*}"

      jarbase_stripped=$(echo "$jarbase" | sed -E 's/[-_]?([0-9]+\.)*[0-9]+$//')
      norm_jarbase=$(echo "$jarbase_stripped" | tr '[:upper:]' '[:lower:]' | sed 's/[-_]/ /g')

      echo "Trying to match job [$norm_jobname] against jar [$norm_jarbase]"

      if [[ "$norm_jobname" == *"$norm_jarbase"* ]]; then
        matched_jar="$jarbase"
        matched_norm_base="$norm_jarbase"
        break
      fi
    done

    dir_safe_name=$(echo "${matched_jar:-$jobname}" | sed 's/[^a-zA-Z0-9._-]/_/g')
    save_dir="$SAVEPOINT_DIR/$dir_safe_name"
    local_save_dir="$SAVEPOINT_LOCAL_DIR/$dir_safe_name"

    echo "Triggering savepoint for job $jobid into $save_dir"
    savepoint_path=$($FLINK_BIN savepoint "$jobid" "$save_dir" | grep "Savepoint completed" | awk '{print $NF}')

    if [ -n "$matched_norm_base" ] && [ -n "$savepoint_path" ]; then
      echo "Writing to jobmap: $matched_norm_base $jobid $savepoint_path"
      echo "$matched_norm_base|$jobid|$savepoint_path" >> "$JOBMAP_FILE"
    else
      echo "Could not match job [$jobname] to any JAR. Skipping savepoint entry."
    fi

    echo "Job name normalized: $norm_jobname"
    echo "Jar match candidate: $norm_jarbase"
  done
}

function cancel_all() {
  echo "Cancelling all running jobs..."
  jobIds=$($FLINK_BIN list -r | grep -oE '\b[0-9a-f]{32}\b')
  if [ -z "$jobIds" ]; then
    echo "No jobs to cancel."
    return
  fi

  for jobId in $jobIds; do
    echo "Cancelling job $jobId ..."
    $FLINK_BIN cancel "$jobId"
  done
}

function restart_scala() {
  echo "Savepointing all Flink jobs..."
  savepoint_all

  echo "Cancelling all jobs..."
  cancel_all

  echo "Waiting 5 seconds for jobs to stop..."
  sleep 5

  echo "Restarting all Flink jobs..."
  find "$JAR_DIR" -type f -name "*.jar" | while IFS= read -r jarfile; do
    jarname=$(basename "$jarfile")
    jarbase="${jarname%.*}"

    # Skip plugin and original jars
    if [[ "$jarname" =~ ^plugin-.*\.jar$ || "$jarname" =~ ^original-.*\.jar$ ]]; then
      echo "Skipping excluded JAR: $jarname"
      continue
    fi

    # Normalize jarbase for matching
    jarbase_stripped=$(echo "$jarbase" | sed -E 's/[-_]?([0-9]+\.)*[0-9]+$//')
    norm_jarbase=$(echo "$jarbase_stripped" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/ /g')

    echo "Looking for match for jar: $norm_jarbase"
    cat "$JOBMAP_FILE"

    savepoint_path=""
    while IFS='|' read -r map_jarbase map_jobid map_path; do
      if [[ "$norm_jarbase" == "$map_jarbase" ]]; then
        savepoint_path="$map_path"
        break
      fi
    done < "$JOBMAP_FILE"

    if [ -n "$savepoint_path" ]; then
      echo "Submitting $jarfile with savepoint: $savepoint_path"
      USING_SAVEPOINT="--fromSavepoint $savepoint_path"
    else
      echo "Submitting $jarfile without savepoint."
      USING_SAVEPOINT=""
    fi

    "$FLINK_BIN" run -d \
      $USING_SAVEPOINT \
      -Denv.java.opts="--add-opens=java.base/java.lang=ALL-UNNAMED" \
      -Denv.SCHEMA_REGISTRY_SERVER="$SCHEMA_REGISTRY_SERVER" \
      -Denv.GROUP_ID="$GROUP_ID" \
      -Denv.KAFKA_SERVER="$KAFKA_SERVER" \
      -Denv.DATABASE_SERVER="$DATABASE_SERVER" \
      -Denv.DATABASE_USERNAME="$DATABASE_USERNAME" \
      -Denv.DATABASE_PASSWORD="$DATABASE_PASSWORD" \
      "$jarfile"
  done

  echo "All jobs restarted."
}

case "$1" in
  list) list_jobs ;;
  savepoint) savepoint_all ;;
  cancel) cancel_all ;;
  restart) restart_scala ;;
  *)
    echo "Usage: $0 {list|savepoint|cancel|restart}"
    exit 1
    ;;
esac