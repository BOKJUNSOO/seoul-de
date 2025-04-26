#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "빌드 시작"

docker compose down
docker compose up --build -d

echo "빌드 완료!"