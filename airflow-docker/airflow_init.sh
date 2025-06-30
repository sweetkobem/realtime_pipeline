#!/usr/bin/env bash

# Exit on error
set -e

if [[ -z "${AIRFLOW_UID}" ]]; then
  echo
  echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
  export AIRFLOW_UID=$(id -u)
fi

one_meg=1048576
mem_available=$(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / one_meg))
cpus_available=$(grep -cE 'cpu[0-9]+' /proc/stat)
disk_available=$(df / | tail -1 | awk '{print $4}')
warning_resources="false"

if (( mem_available < 4000 )) ; then
  echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
  warning_resources="true"
fi

if (( cpus_available < 2 )); then
  echo -e "\033[1;33mWARNING!!!: Not enough CPUs available for Docker.\e[0m"
  warning_resources="true"
fi

if (( disk_available < one_meg * 10 )); then
  echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"
  warning_resources="true"
fi

if [[ ${warning_resources} == "true" ]]; then
  echo -e "\033[1;33mWARNING!!!: You may not have enough resources to run Airflow (see above)!\e[0m"
fi

mkdir -p /opt/airflow/{logs,dags,plugins,config}

echo "Airflow version:"
/entrypoint airflow version

echo "Running airflow config list"
/entrypoint airflow config list >/dev/null

echo "Change ownership to ${AIRFLOW_UID}:0"
chown -R "${AIRFLOW_UID}:0" /opt/airflow/
chown -R "${AIRFLOW_UID}:0" /opt/airflow/{logs,dags,plugins,config}