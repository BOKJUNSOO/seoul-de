### port 정보

airflow port : 8081\
postgreSQL port : 5432


### project run
- 컨테이너 빌드
```
git clone https://github.com/BOKJUNSOO/seoul-de.git
docker compose up -d
```

- `localhost:8081` 접속
  
```
id: airflow
pw: airflow
```

- 키값 설정
```
1. 접속후 상단네비게이션 바에서 `Admin` 선택
2. `Variable` 접속후 `+` 로 키값 설정
3. `key` 란에 꼭! `seoul_api_key`라고 작성, `Val` 란에 발급받은 키 작성
```

### DB 설정

컨테이너 내부에서 `schema`와 `database`를 미리 만들어야 합니다.\
저장할 이름은 `refine_data.py` 에서 설정해줄 수 있습니다.
- 컨테이너 진입
```
docker exec -it <컨테이너명> /bin/bash
```
---
- `airflow` 유저로 사용
```
psql -U airflow
```
---
- `backend` database 생성 (메타데이터와 따로 관리하기 위해)
```
CREATE DATABASE seoulmoa;
```
---
- `seoulmoa` 데이터베이스 접속 및 `datawarehouse` 스키마 생성
- 테이블은 코드 자체적으로 생성을하고 지정한 스키마에 저장합니다.
```
\c seoulmoa
CREATE SCHEMA datawarehouse;
```
---

### airflow run!

`localhost:8081` -> `DAGs` 에서 `datapipline_with_seoul_data` 선택 및 실행

### Note
`get_culture_data` 모듈에서 요청을 조절할 수 있습니다.\
테스트 환경에서 하드코딩을 해놓은 상태입니다.\
