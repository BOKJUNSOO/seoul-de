import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import openai
from common.base.util.helper import corret_lat_lot
from datetime import datetime

def event_data(type_,**kwargs):
    """
    Event 테이블을 생성하는 airflow task 함수

    airflow task instance에서 dataframe을 pull 하여 정제한다.
    schema에 맞게(컬럼명조정) 정제한 테이블을 postgreSQL에 저장한다.
    """
    print("start refine task!")
    ti = kwargs['ti']
    # pull task instance
    df = ti.xcom_pull(key='row_dataframe')
    # refine
    df["BOOL_FEE"] = df["IS_FREE"].map({'유료':False,
                                       '무료':True})
    
    df["STRTDATE"] = pd.to_datetime(df["STRTDATE"])
    df["STRTDATE"] = df["STRTDATE"].dt.date
    df["END_DATE"] = pd.to_datetime(df["END_DATE"])
    df["END_DATE"] = df["END_DATE"].dt.date
    # add column
    df["ROW_NUMBER"] = range(1,len(df)+1)
    
    # refine column
    condtion = df['CODENAME'].str.contains("축제", na = False)
    df.loc[condtion,'CODENAME'] = "축제"

    df = df.apply(corret_lat_lot, axis=1)
    
    # follow schema
    if type_ == "sync":
        columns = ['ROW_NUMBER','TITLE','CODENAME','GUNAME','PLACE','STRTDATE','END_DATE','USE_FEE','BOOL_FEE','LAT','LOT','HMPG_ADDR','MAIN_IMG','ORG_LINK','USE_TRGT']    
        df = df[columns]
        df = df.rename(columns={
            'ROW_NUMBER':'event_id',
            'TITLE':'title',
            'CODENAME':'category_name',
            'GUNAME':'gu',
            'PLACE':'location',
            'STRTDATE':'start_date',
            'END_DATE':'end_date',
            'USE_FEE':'fee',
            'BOOL_FEE':'is_free',
            'LAT':'longitude',
            'LOT':'latitude',
            'HMPG_ADDR':'homepage',
            'MAIN_IMG':'image_url',
            'ORG_LINK':'detail_url',
            'USE_TRGT':'target_user'
        })
    
    elif type_=="init":
        columns = ['ROW_NUMBER','TITLE','CODENAME','GUNAME','PLACE','STRTDATE','END_DATE','USE_FEE','BOOL_FEE','LAT','LOT','HMPG_ADDR','MAIN_IMG','ORG_LINK','USE_TRGT','ALT']
        df = df[columns]
        df = df.rename(columns={
            'ROW_NUMBER':'event_id',
            'TITLE':'title',
            'CODENAME':'category_name',
            'GUNAME':'gu',
            'PLACE':'location',
            'STRTDATE':'start_date',
            'END_DATE':'end_date',
            'USE_FEE':'fee',
            'BOOL_FEE':'is_free',
            'LAT':'longitude',
            'LOT':'latitude',
            'HMPG_ADDR':'homepage',
            'MAIN_IMG':'image_url',
            'ORG_LINK':'detail_url',
            'USE_TRGT':'target_user',
            'ALT':'event_description'
        })
    ti.xcom_push(key='refine_dataframe',value=df)
    print("refine task done!")


def check_event_description(type_,**kwargs) -> dict:
    """
    처음 event 테이블을 구성 및 event_sync에서 
    airflow task의 refine_dataframe을 인자로 받는다.
    
    description의 길이가 특정 조건을 만족하지 못하고, 진행 + 진행할 행사 정보를 추출한다.
    row_number과 hompage 링크를 키벨류쌍으로 기록하여 딕셔너리로 리턴한다.
    """
    ti = kwargs["ti"]
    df = ti.xcom_pull(key='refine_dataframe')
    BATCH_DATE = datetime.strptime(kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d"), "%Y-%m-%d").date()
    
    # 진행중인 행사만 필터링
    condition1 = df['end_date'] >= BATCH_DATE
    df = df.loc[condition1]

    if type_ == "init":
        # 파싱해온 description (alt 에 작성된 내용)의 길이가 너무 짧은 row 필터링(event 전용)
        condition2 = df['event_description'].str.len() <=200
        df = df[condition2]
        # 정보없는 페이지 필터링
        condition3 = df['event_description'] != '정보없음'
        df = df[condition3]
    if type_ == "sync":
        # read 한 event 테이블과 비교했을때 hompage 기준으로 새롭게 존재하는 row만 찾아서 df로 반환하는 함수
        event_df = ti.xcom_pull(key="event")
        event_df.info()
        print("debugging")
        df.info()
        df = check_diff(df,event_df)
        # event init 모드와 다르게 덮어써서 사용한다.
        ti.xcom_push(key="refine_dataframe",value=df)

    target_dict = {}
    for _,row in df.iterrows():
        target_dict[row['event_id']] = row['homepage'] # 다시 저장할 내용 정리
    
    ti.xcom_push(key="research_dict",value=target_dict)
    print("let`s start re_search")

def re_search_function(**kwargs)->dict:
    """
    OpenAI에게 요약을 요청할 html 문서를 파싱해오는 함수
    rownumber와 homepage 딕셔너리를 pull 하여 해당 딕셔너리에 파싱해온 HTML을 할당한다.

    AI에게 전달할 HTML문서를 키벨류쌍으로 저장한 딕셔너리를 push한다.
    """
    ti = kwargs['ti']
    research_dict = ti.xcom_pull(key="research_dict")
    parsing_dict={}
    for event_id, url in research_dict.items():
        for retry in range(4):
            try:
                response = requests.get(url)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, 'html.parser')
                culture_divs = soup.find_all('div',class_='culture-content')
                answer=""
                for div in culture_divs:
                    spans = div.find_all('span')
                    for span in spans:
                        answer+=span.get_text()
                parsing_dict[event_id] = answer
                break
            except requests.exceptions.RequestException as e:
                print(f"[예외발생] {e}, 10초뒤 요청을 다시 합니다{retry}/4.")
                time.sleep(10)
                continue
    print("xcom에서 ai에게 요약 요청할 정보를 확인할 수 있습니다.")
    ti.xcom_push(key='html_dict',value=parsing_dict)


def make_summary_ai(OPEN_AI_KEY,**kwargs):
    """
    OPEN AI 키를 필요로한다.
    Open ai 에게 요약요청을 하는 함수 파싱에 실패한 페이지의 HTML 문서를 전달한다.
    
    전달한 문서에 대한 응답을 받아 row_number에 맞게 값을 저장하고, 리턴하는 함수
    기존에 메모리에 존재하는 event 테이블의 값을 대치시킨다.
    """
    # gpt 인스턴스 생성
    openai_client = openai.OpenAI(api_key=OPEN_AI_KEY)
    ti = kwargs['ti']
    result_dict = ti.xcom_pull(key="html_dict")
    df = ti.xcom_pull(key="refine_dataframe")
    # 모아온 html을 차례대로 요청 및 예외처리
    for idx, text in result_dict.items():   
        prompt = f"""
        다음 텍스트는 html 페이지에서 특정 태그를 파싱해온 결과야.
        - 새로운 정보를 추가하지 말고 요약해줘.
        - 요약결과는 300자 내외를 지켜줘.
        - 꼭 경어체를 사용해줘.
        
        {text}
        """
        for retry in range(4):
            try:
                response = openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "너는 텍스트 요약 전문가야"},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0,
                    max_tokens=4000
                )
                answer = response.choices[0].message.content
                result_dict[idx] = answer
                print(f"AI 요약 결과입니다:{answer}")
                break
            except Exception as e:
                print(f"OPEN AI 요청 에러 발생:{e} 10초뒤 재시도 합니다.{retry}/4")
                continue
    
    # 기존 데이터 프레임에 description 대치!
    df.info()
    print(result_dict)
    for _, row in df.iterrows():
        event_id = row['event_id'] # event_id : int type

        if str(event_id) in result_dict: # result_dict key : string type
            df.loc[df['event_id'] == event_id,'event_description'] = result_dict[str(event_id)]

    ti.xcom_push(key="to_save_data",value=df)

def check_diff(df,event_df)->pd.DataFrame:
    """
    task instance에 존재하는 sync 테이블과 event 테이블을 비교하고
    존재하지 않은 행사만 필터링한다.
        - 필터링한 후 refine_dataframe으로 저장한다.
    args:
        df : 이미 batch 기간 기준으로 필터링이 완료된 sync 프레임
        event_df : (DB에서 읽어온 event 테이블)

    return:
        dataframe
    """
    existing_homepages=set(event_df['homepage'])
    
    new_rows = df[~df['homepage'].isin(existing_homepages)]
    new_rows.info()
    return new_rows