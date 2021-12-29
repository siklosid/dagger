#!/usr/bin/env bash

set -u

TRY_LOOP="20"

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z $host $port >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

configure_airflow() {
  airflow users create -r Admin -u dev_user -e dev@user.com -f Dev -l User -p dev_user

}

id
ls -lh ${AIRFLOW_HOME}/dags/

if [ "$AIRFLOW__CORE__EXECUTOR" != "SequentialExecutor" ]; then
  wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
fi

case "$1" in
  webserver)
    airflow db init
    airflow db upgrade
    configure_airflow
    exec airflow webserver
    ;;
  scheduler)
    airflow db check-migrations -t 600
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
