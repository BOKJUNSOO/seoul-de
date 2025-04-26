#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "빌드 시작"

docker compose down

echo "airflow.cfg 상태 체크 중..."

if [ -d ./config/airflow.cfg ]; then
    echo "airflow.cfg가 디렉토리입니다. 삭제합니다."
    rm -rf ./config/airflow.cfg
fi

docker compose up --build -d

echo "빌드 완료!"