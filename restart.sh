#!/bin/bash

echo "재시작"

docker compose down
docker compose up -d

echo "재시작 완료!"