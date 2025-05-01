### port 정보

airflow port : 8081\
postgreSQL port : 5432


### project run
- 컨테이너 빌드
```
git clone https://github.com/BOKJUNSOO/seoul-de.git
docker compose up -d
```

### Airflow 설정
총 `2가지` 해주셔야 합니다.
#### 1. api 키 설정
- `localhost:8081` 접속
  
```
id: airflow
pw: airflow
```

- 키값 설정
`OPEN AI 와 서울열린광장 + 공공데이터 키를 발급 받으셔야 합니다!`

```
1. 접속후 상단네비게이션 바에서 `Admin` 선택
2. `Variable` 접속후 `+` 로 키값 설정
3. `key` 란에 꼭! `seoul_api_key`라고 작성, `Val` 란에 발급받은 키 작성
4. `key`를 `OPEN_AI_KEY_secret` 라고 작성, `Val` 란에 발급받은 키 작성
5. `key`를 `portal_key_secret` 라고 작성, `Val` 란에 발급받은 키 작성
```
---

#### 2. connection 설정


1. 접속후 상단에 `Admin` 선택
2. `Connections` 접속후 `+` 로 connection 변수 설정

- 대소문자 확인후 아래와 같이 작성해주세요
```
`Connection Id` : seoul_moa_event_conn
`Connection Type`: Postgres
`Host` : postgres
`Database`: seoulmoa
`Login` : airflow // 아마 적혀있을 겁니다.
`Port` : 5432
```
<br>

---

<br>

### DB 설정


컨테이너 내부에서 `schema`와 `database`를 미리 만들어야 합니다.\
저장할 이름은 `refine_data.py` 에서 설정해줄 수 있습니다.
- 컨테이너 진입
```
docker exec -it <컨테이너명> /bin/bash
```
---
- PostGIS 확장 설치
```
apt update
apt install -y postgis postgresql-13-postgis-3
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
- PostGIS 확장 활성화
```
CREATE EXTENSION postgis;
```
---

### airflow run!

`localhost:8081` -> `DAGs` 실행

### Note
`get_culture_data` 모듈에서 요청을 조절할 수 있습니다.\
테스트 환경에서 하드코딩을 해놓은 상태입니다.\
