<div align="center">
   <h1> 서울모아  </h1>
   <h3> 메인 서비스 테이블 구축 자동화 </h3>
   <h3> Airflow Application</h3>
   <img src='https://github.com/user-attachments/assets/4e97e1f0-d9ac-46b8-bdb3-4d6bef53e5d0'>
</div>

## 📢 프로젝트 소개


`#ETL` `#MLops` `#행사정보 AI 요약본` `#BATCH` `#Airflow`

<br>

서울시 행사정보에 대한 `AI 요약본` 제공 하며 서비스에 필요한 데이터를 DB에 적재합니다.<br>

월별 지하철 데이터를 수집해 일일단위 `시간대별` 지하철 사용량 예측결과를 제공합니다.<br>

매 시간 날씨 데이터 정보를 수집하여 행정구역별 예측 정보 테이블을 생성합니다.


<br>
<img width="1000" alt="DAGlist" src="https://github.com/user-attachments/assets/e66b08f9-f049-4769-949d-40dad51eefb6">
<br>

<br>

## 🔎 주요 DAG 소개

<br>

### 🎯 `event_seoul_data DAG` - data flow
초기 데이터셋 구축, 데이터셋 갱신이라는 2가지 flow 를 `BranchOperator` 를 이용하여 구성했습니다.<br>

<br>
<img width="1000" alt="eventDAGgraph" src="https://github.com/user-attachments/assets/e3a61831-ddb8-4f5c-b78c-6a653aa216f8">
<br>

### 🎯 `event_seoul_data DAG` - AI 요약본
`Airflow` 컨테이너와 외부 `Open AI`와의 통신, 작성된 프롬프트를 이용한 요청을 통해 AI 요약본을 서비스에 이용할 수 있고, <br>
`task`의`log`를 통해 금일 갱신된 행사의 갯수, 요약하여 저장될 내용의 모니터링이 가능합니다.<br>

<br>
<img width="1000" alt="loglevel" src="https://github.com/user-attachments/assets/7c254afb-5bb3-4e36-9fde-4f2e064df1b4">
<br>

해당 데이터셋은 서비스에 이용될 API 뿐만 아니라 `chat-moa` 의 데이터셋으로 이용될될 수 있습니다!

### 🎯 `MLops DAG` - data flow
초기 설정에서 수집된 작년도 월별 데이터를 DB에서 읽어오고<br> 
일일단위로 쌓인 `feature`를 이용해 서울시 모든역의 시간대별 사용량을 예측합니다.<br>

<br>
<img width="1000" alt="MLopsDAGgraph" src="https://github.com/user-attachments/assets/061d4424-f532-4f4c-941c-d851c277290a">
<br>

### 🎯 `MLops DAG` - R2_score monitering
일일단위 예측한 데이터의 `R2-score`을 모니터링 할 수 있도록 log 화면을 구성했습니다.<br>

<br>
<img width="1000" alt="MLopsR2score" src="https://github.com/user-attachments/assets/c1598d9f-7d1d-4fb5-82f3-70d368d88846">
<br>

### 🎯 그외 - `weather, station meta data`
날씨 데이터와 지하철 역사 정보(위도, 경도 등 메타 데이터)는 비교적 간단한 flow를 통해 데이터를 수집 및 저장합니다.

<br>
<img width="1000" alt="MLopsR2score" src="https://github.com/user-attachments/assets/4457de4a-531f-4e8e-bbaa-6d837c24ee2a">
<br>

<br>
<img width="1000" alt="MLopsR2score" src="https://github.com/user-attachments/assets/2f382f76-2466-40c3-9058-0e91ea7f66d0">
<br>

<br>

## ⚒️ DE 기술 스택
| 분류 | 기술 |
| ---- | ---- |
| 언어 | <img src= "https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white"> |
| 사용 라이브러리 | <img src= "https://img.shields.io/badge/OpenAI%20Python-000000?style=flat-square&logo=openai"> <img src= "https://img.shields.io/badge/scikit--learn-F7931E?style=flat-square&logo=scikit-learn&logoColor=white"> |
| 워크플로우 오케스트레이션 | <img src= "https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apache-airflow&logoCOlor=auto"> <img src= "https://img.shields.io/badge/PostgreSQL-336791?style=flat-square&logo=postgresql&logoCOlor=white"> |
| 배포 | <img src= "https://img.shields.io/badge/Jenkins-D24939?style=flat-square&logo=jenkins&logoColor=white"> <img src= "https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white"> |

<br>

## 🏆 Seoul-moa 팀원 깃허브

<table align="center" width="100%">
  <tr>
    <td><img src="https://avatars.githubusercontent.com/u/68840464?v=4"/></td>
    <td><img src="https://avatars.githubusercontent.com/u/102515499?v=4"/></td>
    <td><img src="https://avatars.githubusercontent.com/u/108779266?v=4"/></td>
    <td><img src="https://avatars.githubusercontent.com/u/73154551?v=4"/></td>
    <td><img src="https://avatars.githubusercontent.com/u/170912062?v=4"/></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/Hwanjin-Choi">최환진</a>
    </td>
    <td align="center"><a href="https://github.com/seoyoun8694">채서윤</a>
    </td>
    <td align="center"><a href="https://github.com/SoonWookHwang">황순욱</a>
    </td>
    <td align="center"><a href="https://github.com/MingyooLee">이민규</a>
    </td>
    <td align="center"><a href="https://github.com/BOKJUNSOO">복준수</a>
  </tr>
  <tr>
    <td align="center">Front-End</td>
    <td align="center">Front-End</td>
    <td align="center">Back-End</td>
    <td align="center">Back-End & Deploy</td>
    <td align="center">Data Engineering</td>
  </tr>
</table>

<br>

## 🎈 Project repo

[프론트엔드 서버 레포는 여기!](https://github.com/Hwanjin-Choi/project-seoul-moa-frontend)

[백엔드 서버 레포는 여기!](https://github.com/SoonWookHwang/seoul-moa)

[데이터 파이프라인 서버는 여기!](https://github.com/BOKJUNSOO/seoul-de)

[chat moa 서버는 여기!](https://github.com/BOKJUNSOO/seoul-chat-moa)

<br>

## 🔥 Data Engineering 에서의 도전 과제
해당 프로젝트에서 `ETL` 프로세스를 보다 더 <br>

`가볍고` `유연하게` 를 핵심가치로 삼았고

수집 및 처리 이후에 저장되는 데이터가\
서비스 구성과 퀄리티에 직결되다 보니<br>

`바로 사용할수 있는 데이터셋을 구축하기!`\
를 중심적으로 프로세스를 구성했습니다.\
(cf 해당 부분이 해결되니 챗봇에 사용할 임베딩 데이터셋은 덤!)

따라서 다음과 같은 부분을 고려해야 했습니다.
```
세부적인 내용은 아래 링크의 velog 링크에서 확인할 수 있습니다!
```

1. 서버 리소스의 최적화 `(인프라 레벨)`\
[오버엔지니어링 잡기](https://velog.io/@junsoobok/%EC%84%9C%EB%B2%84-%EB%A6%AC%EC%86%8C%EC%8A%A4)

1. 코드의 모듈화 및 재사용 가능성 `(코드 레벨)`\
[DAG 개발 및 코드의 모듈화](https://velog.io/@junsoobok/2-%EC%BD%94%EB%93%9C%EC%9D%98-%EB%AA%A8%EB%93%88%ED%99%94)

- 백엔드를 맡아주신 팀원과의 소통
- 코드의 재사용성과 모듈화

3. 데이터 수집단계, 행사정보 파싱 `(코드 레벨)`\
[AI 도입기](https://velog.io/@junsoobok/3-HTML-%ED%8C%8C%EC%8B%B1
)\
[구축한 데이터셋 RAG에 이용하기](https://velog.io/@junsoobok/chat-moa-Retrieval)

AI 도입전 
```
2025 금천하모니축제  
오늘의 어울림, 내일의 더울림  금천 30년  금천하모니축제 | 금천하모니 Week 금천하모니축제 | 본행사  금천하모니 
Week 10.11.(토)-10.19.(일)  
개·폐막공연·체험프로그램 금천30주년 기념 전시 
미디어아트+드론쇼  금천구청광장,  
금천구 전역 안양천다목적광장  
* 주간행사  푸드코너 플리마켓
```

AI 도입후!
```
금천하모니축제는 2025년을 향해 펼쳐지는 환상적인 축제에요!
주민들이 함께 만들고 즐기는 이 축제는 지역 공동체의 화합을 이끌어내고, 음악을 통해 모두가 하나가 되는 특별한 순간을 만들어요.
이 축제는 합창을 중심으로 한 독창적인 프로그램으로 구성되어 있어요.
또한 금천구만의 브랜드 가치를 높여 지역을 대표하는 문화축제로 자리매김하고자 해요.
금천하모니 Week는 10월 11일부터 19일까지 열리며, 금천구 전역에서 문화예술의 향연이 펼쳐질 거예요.
본행사는 10월 18일부터 19일까지 금천구청광장과 안양천다목적광장 일대에서 열려요.
이 축제에서는 다양한 프로그램과 체험이 기다리고 있어요.
더불어 30주년을 맞이하는 금천을 기념하는 전시도 마련되어요.
이번 축제에서는 미디어아트와 드론쇼도 즐길 수 있답니다.
금천하모니축제로 함께 어울리고, 내일을 더울리며 즐거운 시간을 보내보세요!
```

<br>

## 🔥 MLops - 지하철 사용량 예측!


1. 모델링 (데이터 모델링 및 가설설정)
   
   [그래서 어떻게 할건가?](https://velog.io/@junsoobok/MLops-%EB%AA%A8%EB%8D%B8%EB%A7%81)

2. 데이터 분석 및 엔지니어링
   
   [그래서 어떻게 했는가?](https://velog.io/@junsoobok/MLops-DAG-%EA%B5%AC%EC%84%B1-%EB%B0%8F-%EA%B5%AC%ED%98%84)

   [OOM trouble shooting](https://velog.io/@junsoobok/MLops-trouble-shooting)
- 데이터 분석 및 시각화
- MLops 파이프라인 구성
- task instance OOM - trouble shooting

3. output

<img src='https://github.com/user-attachments/assets/07c4d104-f3a1-4ca0-8d62-184b5da04d21'>

<br>

## 😎 프로젝트 실행

### port info

airflow prot : 8081\
postgreSQL port : 5432

### project run
```bash
git clone https://github.com/BOKJUNSOO/seoul-de.git
docker compose up --build -d
```

<br>

## 😎 프로젝트 사용시 설정 사항

- 서울 열린데이터 광장 키 발급
- 공공데이터 포털 기상청 데이터 키 발급
- OPEN AI 키 발급

### 1. KEY 셋팅
Airflow의 Variable 모듈의 보안 기능을 활용했습니다.<br>
이에 프로젝트를 빌드한후 `localhost:8081` 에 접속합니다.<br>
접속 이후에 상단 네비게이션 바 `Admin -> Variables` 접속

<br>
<img alt="setting1" src="https://github.com/user-attachments/assets/9d4e6b83-b504-4a81-8edb-b8fabda0b54f">
<br>

`+` 키를 클릭한 후 발급받은 키를 설정해줍니다.<br>
`Key`에는 꼭! 명시된 값을 입력해 주세요<br>

>서울시 열린데이터 광장 : `seoul_api_key`<br>
>OPEN AI : `OPEN_AI_KEY_secret`<br>
>공공 데이터 포털 : `portal_key_secret`

`Val`에는 발급받은 키를 입력합니다.

<br>
<img width="1000" alt="setting" src="https://github.com/user-attachments/assets/67cbcfe3-a3da-4826-9b90-4d2713e4e013">
<br>

### 2. DB 셋팅

- 컨테이너 진입
```bash
docker exec -it <postgres 컨테이너명> /bin/bash
```
- PostGIS 확장 설치
```bash
apt update
apt install -y postgis postgresql-13-postgis-3
```

- `airflow` 유저 사용
```bash
psql -U airflow
```

- database 생성
```bash
CREATE DATABASE seoulmoa;
```

- `seoulmoa` 데이터베이스 접속 및 `datawarehouse` 스키마 생성
```bash
\c seoulmoa
CREATE SCHEMA datawarehouse;
```

- PostGIS 확장 활성화
```bash
CREATE EXTENSION postgis;
```

### 3. connection 생성
event_sync 테이블에 대한 메타데이터를 INSERT하기 위해 connection 을 설정합니다.<br>

`Admin` 의 `Connections` 탭을 선택합니다.<br>

<br>
<img alt="setting" src="https://github.com/user-attachments/assets/f671ffa7-2c1b-42d9-9cda-a46f39246e0b">
<br>

`+` 버튼을 누르고 새로운 connection 을 생성합니다.<br>
```
Connetion Id : seoul_moa_event_conn
Connection Type : Postgres
Host : postgres
Database : seoulmoa
Login : airflow
Port : 5432
```
<br>
<img width="1000" alt="setting" src="https://github.com/user-attachments/assets/ce3c3892-0187-4ec4-9518-d9089dceafcf">
<br>
