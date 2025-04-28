import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import openai
from common.base.util.helper import corret_lat_lot
from datetime import datetime

def event_data(type_,**kwargs):
    """
    공공데이터 API 호출이 끝난 이후 row dataframe을 정제하는 함수 refine_data_* 테스크에 이용
    flow의 흐름을 정리하기 위해 존재하는 중간단계의 함수
    **중요** : 공공 API 에 LOT 과 LAT 이 반대로 표기. 이에 맞춰 정제한다.

    args:
        type_ : "init" or "sync"
            - init : event 테이블이 없는 경우에 인자로 전달된 테스크가 실행된다. (refine_data)
            - sync : event 테이블이 있는 경우에 인자로 전달된 테스크가 실행된다. (refine_data_s)
        pull key : row_dataframe
            - API 호출이 끝난후 row_dataframe을 sync 혹은 init에 맞게 정제한다.
            - 이는 추후 check_event_description_* task에 영향
    return : 
        push key : refine_dataframe
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
    event descrition을 확인하고 추가로 필요한 homepage + row를 키벨류쌍으로 리턴하는 함수

    args:
        type_:
            init : 처음 파싱해온 description 의 타당도 여부를 결정하고 진행중인 이벤트를 기준으로 dictionary 리턴
            sync : refine_dataframe 을 새롭게 정의하고(어제는 존재하지 않던 row), descrition을 요청할 dictionary 리턴
        pull key : 
            -refine_dataframe (both)
                - API 호출이 끝난후 row_dataframe을 sync 혹은 init에 맞게 정제한다.
                - 이는 추후 check_event_description_* task에 영향
            -event (only sync.)
                - event table과 신규 테이블 비교 (read event table task가 있을 경우만 사용)
    return : 
        push key : refine_dataframe (only sync.)
        push key : research_dict (both)
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
        df.info()
        df = check_diff(df,event_df)
        # 기존의 refine_dataframe을 사용하지 않는다.
        # init 모드의 경우 해당 키값을 사용해야 한다.
        ti.xcom_push(key="refine_dataframe",value=df)

    target_dict = {}
    for _,row in df.iterrows():
        target_dict[row['event_id']] = row['homepage'] # 다시 저장할 내용 정리
    
    ti.xcom_push(key="research_dict",value=target_dict)
    print("let`s start re_search")

def re_search_function(**kwargs)->dict:
    """
    requesets를 통해 AI에게 요약을 부탁할 문자열을 파싱해오는 함수
    culture content의 span에 존재하는 문자열, alt에 붙어있는 설명을 모두 가져온다.

    args:
        pull key : 
            - research_dict : check_event_descrition 함수의 리턴값, 필터링된 행사의 홈페이지, row넘버
    return : 
        push key : html_dict - ai에게 요약을 요청할 html 문서값과 로우넘버의 벨류쌍
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

                    # alt 속성 추출
                    tags_with_alt = div.find_all(attrs={"alt": True})  # alt 속성이 있는 모든 태그
                    for tag in tags_with_alt:
                        alt_text = tag.get('alt')
                        if alt_text:  # alt 값이 존재할 때만 추가
                            answer += alt_text
                parsing_dict[event_id] = answer
                break
            except requests.exceptions.RequestException as e:
                print(f"[예외발생] {e}, 10초뒤 요청을 다시 합니다{retry}/4.")
                time.sleep(10)
                continue
    print("xcom에서 ai에게 요약 요청할 정보를 확인할 수 있습니다.")
    print("ai에게 요약 요청할 row의 갯수 :",len(parsing_dict))
    ti.xcom_push(key='html_dict',value=parsing_dict)


def make_summary_ai(OPEN_AI_KEY,**kwargs):
    """
    requesets를 통해 AI에게 요약을 부탁할 문자열을 파싱해오는 함수
    culture content의 span에 존재하는 문자열, alt에 붙어있는 설명을 모두 가져온다.

    args:
        OPEN_AI_KEY : Open AI 키
        pull key : 
            - html_dict : re_search_function 함수의 리턴값, 행사의 홈페이지의 파싱정보와 row넘버
            - refine_dataframe:
                init 단계에서는 기본 event 테이블
                sync 단계에서는 기존에 존재하지 않던 row들의 table
    return : 
        push key : to_save_data - 최종 저장소에 저장하게 되는 인스턴스
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
        - 새로운 정보를 추가하지 말고 있는 정보를 정리해줘.
        - 밝은 느낌으로 내용을 소개해줘.
        - 꼭 경어체를 사용해줘.
        
        {text}
        """
        for retry in range(4):
            try:
                response = openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "너는 텍스트 정리 전문가야"},
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