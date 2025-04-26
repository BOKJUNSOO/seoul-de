#!/bin/bash
set -e

if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

echo "빌드 시작"

docker compose down
docker compose up --build -d

echo "빌드 완료!"