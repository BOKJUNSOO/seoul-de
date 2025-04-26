#!/bin/bash
set -e

if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

echo "재시작"

docker compose down
docker compose up -d

echo "재시작 완료!"