def event_data(**kwargs):
    import pandas as pd
    from datetime import datetime
    """
    공공데이터 API 호출이 끝난 이후 row dataframe을 정제하는 함수 refine_data_* 테스크에 이용
    flow의 흐름을 정리하기 위해 존재하는 중간단계의 함수
    **중요** : 공공 API 에 LOT 과 LAT 이 반대로 표기. 이에 맞춰 정제한다.

    args:
        pull key : 
        - row_dataframe
            - API 호출이 끝난후 row_dataframe을 sync 혹은 init에 맞게 정제한다.
            - 이는 추후 check_event_description_* task에 영향
        - key
            - sync 테이블은 끝나지 않은 행사만 업데이트 시킨다.
        
    return : 
        push key : refine_dataframe
    """

    print("[INFO] - this task is rename columns or refine data for service")
    ti = kwargs['ti']
    # pull task instance
    df = ti.xcom_pull(key='row_dataframe')
    BATCH_DATE = datetime.strptime(kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d"), "%Y-%m-%d").date()
    type_ = ti.xcom_pull(key='key')
    print("[INFO] - xcom_pull - key : row_dataframe, value : dataframe")
    print(f"[INFO] - xcom_pull - key : key, value : {type_}")

    # refine
    df["BOOL_FEE"] = df["IS_FREE"].map({'유료':False,
                                       '무료':True})
    
    df["STRTDATE"] = pd.to_datetime(df["STRTDATE"])
    df["STRTDATE"] = df["STRTDATE"].dt.date
    df["END_DATE"] = pd.to_datetime(df["END_DATE"])
    df["END_DATE"] = df["END_DATE"].dt.date
    

    # sync type 이라면 끝나지 않은 행사만 필터링
    # row number 할당 이전에 처리
    if type_=='sync':
        print("[INFO] - sync table`s date is filtering")
        condition1 = df['END_DATE'] >= BATCH_DATE
        df = df.loc[condition1]
    
    # add column
    df["ROW_NUMBER"] = range(1,len(df)+1)
    
    # refine column
    condtion = df['CODENAME'].str.contains("축제", na = False)
    df.loc[condtion,'CODENAME'] = "축제"
    
    # follow schema
    columns = ['ROW_NUMBER','TITLE','CODENAME','GUNAME','PLACE','STRTDATE','END_DATE',
               'USE_FEE','BOOL_FEE','LAT','LOT','HMPG_ADDR','MAIN_IMG','ORG_LINK','USE_TRGT','ALT']
    
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
    print("[INFO] - xcom_push - key : refine_dataframe, value : dataframe")
    print("[INFO] - refine task done!")

# check_daily_ task 의 값을 결정하는 함수
def check_daily(**kwargs):
    """
    공공데이터 API 호출이 끝난 이후 실행되는 BranchOperator 함수
    flow의 흐름을 정리하기 위해 존재하는 중간단계의 함수
    args:
        pull key : key
            - 메인 파이프라인 시작 task에서 check table 이후(repository 메서드) 메모리에 존재하는 값을 이용한다.
            - 해당 task에서 event 처리인지 sync 처리인지가 결정되어 있다.
    
    return : 
        status : string
            - 다음 테스크를 결정해준다.
            - refine_data_ 혹은 refine_data_s task 둘중 하나의 값을 가진다.
            - 전자의 경우 event 테이블 정제/ 후자의 경우 event sync 테이블 정제
    """
    ti = kwargs['ti']
    type_ = str(ti.xcom_pull(task_ids='check_data_',key='key'))
    print("[INFO] - this task is branch task")
    print(f"[INFO] - xcom_pull - key : key, value : {type_}")
    
    if type_ == "init":
        status = "check_event_description_i_"
    
    if type_ == "sync":
        status = "read_event_table_"
    
    print(f"[INFO] - return value : {status}")
    print("[INFO] check task done!")
    return status

def check_event_description(**kwargs) -> dict:
    from datetime import datetime
    """
    event descrition을 확인하고 추가로 필요한 homepage + row를 키벨류쌍으로 리턴하는 함수

    args:
        pull key : 
            -refine_dataframe (both)
                - API 호출이 끝난후 row_dataframe을 sync 혹은 init에 맞게 정제한다.
                    - sync : 기존에 존재하지 않던 row 만 row number : homepage 리턴
                    - init : 행사 진행 기준으로 필터링 및 200자 이하 description의 row number : homepage 리턴
                - 이후 push 하지 않는다!
            
            -key
                - DAG 첫 페이즈의 key 값을기준으로 판다 (init or sync)

            -event (only sync.)
                - event table과 신규 테이블 비교
    return : 
        push key : research_dict (both)
    """
    target_dict = {}
    ti = kwargs["ti"]
    df = ti.xcom_pull(key='refine_dataframe')
    type_ = ti.xcom_pull(key='key',task_ids='check_data_')
    BATCH_DATE = datetime.strptime(kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d"), "%Y-%m-%d").date()
    
    print("[INFO] - check new event task")
    print("[INFO] - this task can filtering event table that need summary with return dictionary")
    print(f"[INFO] - xcom_pull - key : key, value : {type_}")
    print(f"[INFO] - xcom_pull - key : refine_dataframe, value : dataframe")

    if type_ == "init":
        # init 의 경우 날짜 필터링이 아직 진행되지 않음, row number 유지를 위해 해당 단계에서 필터링
        condition1 = df['end_date'] >= BATCH_DATE
        df = df.loc[condition1]

        # 정보없는 페이지 필터링
        condition2 = df['event_description'] != '정보없음'
        df = df[condition2]

        # 요약할 행사정보의 rownumber, homepage 키벨류쌍 저장
        for _,row in df.iterrows():
            target_dict[row['event_id']] = row['homepage']

        print(f"[INFO] - init table`s row : {len(df)}")
        ti.xcom_push(key="research_dict",value=target_dict)
        print(f"[INFO] - xcom_push - key : research_dict, value : dictionary")

    
    if type_ == "sync":
        print("[INFO] - compare event with table")
        event_df = ti.xcom_pull(key="event")
        print("[INFO] - xcom_pull - key : event")
        new_parsing_row = check_diff(df,event_df)
        
        # refine_dataframe에 해당 요약 요청 row 만 추가한다
        for _,row in new_parsing_row.iterrows():
            target_dict[row['event_id']] = row['homepage']
            print("[INFO] - new homepage :",row['homepage'])
        
        print(f"[INFO] - sync table`s row : {len(df)}")
        print(f"[INFO] - today`s new event count : {len(new_parsing_row)}")
        ti.xcom_push(key="research_dict",value=target_dict)
        print(f"[INFO] - xcom_push - key : research_dict, value : dictionary")

    print("[INFO] - check event description task is done!")

def re_search_function(**kwargs)->dict:
    import requests
    from bs4 import BeautifulSoup
    import time
    """
    requesets를 통해 AI에게 요약을 부탁할 문자열을 파싱해오는 함수
    culture content의 span에 존재하는 문자열, alt에 붙어있는 설명을 모두 가져온다.

    args:
        pull key : 
            - research_dict : check_event_descrition 함수의 리턴값, 필터링된 행사의 홈페이지, row넘버
    return : 
        push key : html_dict - ai에게 요약을 요청할 html 문서값과 로우넘버의 벨류쌍
    """

    print("[INFO] - research task is now running")
    print("[INFO] - this task is make request for event page and parsing string")

    ti = kwargs['ti']
    print(f"[INFO] - xcom_pull key : research_dict, value : dictionary")
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
                print(f"[EXCEPTION] - event-seoul server error{e}")
                print(f"[CATCH] - Retry for four times before 10 seconds.{retry}/4.")
                time.sleep(10)
                continue
    print("[INFO] - you can check for row information with xcom tab.")
    print(f"[INFO] - request row count for AI : {len(parsing_dict)}.")
    ti.xcom_push(key='html_dict',value=parsing_dict)
    print(f"[INFO] - xcom_push - key : html_dict, value : dictionary")
    print("[INFO] - research task is done!")


def make_summary_ai(OPEN_AI_KEY,**kwargs):
    import openai
    import time
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

    print("[INFO] - make summary task is running")
    print("[INFO] - this task is make request for OpenAI and make summary")

    # gpt 인스턴스 생성
    
    openai_client = openai.OpenAI(api_key=OPEN_AI_KEY)
    
    ti = kwargs['ti']
    result_dict = ti.xcom_pull(key="html_dict")
    df = ti.xcom_pull(key="refine_dataframe")
    print(f"[INFO] - xcom_pull - key : html_dict, value : dictionary")
    print(f"[INFO] - xcom_pull - key : refine_dataframe, value : dataframe")
    
    # 모아온 html을 차례대로 요청 및 예외처리
    for idx, text in result_dict.items():   
        prompt = f"""
        The following text is the result of parsing specific tags from an HTML page.  
        - Please do not add any new information; only use the content provided.  
        - Summarize the content in Korean, within around 1000 characters, with a clear beginning, development, turn, and conclusion.
        - Present the summary in a cheerful and light tone.  
        - Be sure to use polite **respectful** language.

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
                print(f"[INFO] - AI summary result {idx}:{answer}")
                break
            except Exception as e:
                print(f"[EXCEPTION] - OPEN AI api server error{e}.")
                print(f"[CATCH] - Retry for four times before 10 seconds.{retry}/4.")
                time.sleep(10)
                continue
    
    # 기존 데이터 프레임에 description 대치!
    for _, row in df.iterrows():
        event_id = row['event_id'] # event_id : int type

        if str(event_id) in result_dict: # result_dict key : string type
            df.loc[df['event_id'] == event_id,'event_description'] = result_dict[str(event_id)]
    
    ti.xcom_push(key="to_save_data",value=df)
    print(f"[INFO] - compare with description task`s table row. : {len(df)}")
    print(f"[INFO] - xcom_push - key : to_save_data, value : dataframe")
    print("[INFO] - summary task is done!")

# check_status_ task 의 값을 결정하는 함수
def check_status(**kwargs):
    """
    AI 요약 이후에 실행되는 BranchOperater 함수
    event 혹은 sync 테이블중 어떤 테이블을 이용할지 String 으로 리턴해준다.
    args:
        pull key : check_data_
            - 메인 파이프라인 시작 task에서 check table 이후(repository 메서드) 메모리에 존재하는 값을 이용한다.
            - 해당 task에서 event 처리인지 sync 처리인지가 결정되어 있다.
    
    return : 
        status : string
            - 다음 테스크를 결정해준다.
            - save_to_event_ 혹은 save_to_sync task 둘중 하나의 값을 가진다.
    """
    
    ti = kwargs['ti']
    type_ = str(ti.xcom_pull(task_ids='check_data_',key='key'))
    print("[INFO] - this task is branch task")
    print(f"[INFO] - xcom_pull - key : key, value : {type_}")
    
    if type_ == 'init':
        status = 'save_to_event_'
        
    if type_ == 'sync':
        status = 'save_to_sync_'
    
    print(f"[INFO] - return value : {status}")
    print("[INFO] - check task done!")
    return status

def check_diff(df,event_df):
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
    mask = ~df['homepage'].isin(event_df['homepage'])
    new_rows = df[mask]
    return new_rows