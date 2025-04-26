#!/bin/bash

echo "재시작"

cd /Users/username/Projects/seoul-de

docker compose down
docker compose up -d

echo "재시작 완료!"