def parse_html(html_page_url,page)->str:
    from bs4 import BeautifulSoup
    import requests
    '''
    div 에서 행사정보(event_description)을 paring 해서 리턴하는 파이썬 함수
    culture-content 클래스의 alt 를 우선적으로 파싱하고, img-box 클래스의 alt를 두번째 순위로 파싱한다.

    args:
        html_page_url : api 요청 과정에서 homepage url을 파싱
        page : 로그확인용 api 요청 page

    return:
        event_description : str
    '''

    response = requests.get(html_page_url)
    soup = BeautifulSoup(response.text,'html.parser')
    img_tag = None
    alt_text1 = None
    alt_text2 = None

    # culture-content의 내용확인
    culture_div = soup.find('div', class_='culture-content')
    if culture_div:
        img_tag = culture_div.find('img')
        if img_tag and img_tag.has_attr('alt'):
            alt_text1 = img_tag['alt']

    # img-box 내용 확인
    img_box = soup.find('div', class_='img-box')
    if img_box:
        img_tag = img_box.find('img')
        if img_tag and img_tag.has_attr('alt'):
            alt_text2 = img_tag['alt']
    
    # 둘다 내용이 존재하고
    if alt_text1 != None and alt_text2 != None:
        # 공백이 아닌 문자열이 둘중에 하나라도 있는경우
        if len(alt_text1.strip())>0 or len(alt_text2.strip()) >0:
            # 더 긴 내용을 선택
            if len(alt_text1.strip()) >= len(alt_text2.strip()):
                alt_text = alt_text1
            elif len(alt_text1.strip()) < len(alt_text2.strip()):
                alt_text = alt_text2
            return alt_text
    
    # 둘중 내용이 존재하지 않는게 있다면
    if alt_text1 != None or alt_text2 != None:
        # 존재하는 내용이면서
        if alt_text1 != None: # alt_text2는 무조건 None이 된다(위에서 필터링)
            # 공백이 아닌 문자열을 선택
            if len(alt_text1.strip()) > 0:
                alt_text = alt_text1
                print(f"[INFO] - {page} 존재하는 내용으로 가져왔어요:",alt_text)
                return alt_text
            # alt_text1 == None이 아니고 alt_text1의 길이가 0인경우(공백)
            else:
                return "정보없음"

        elif alt_text2 != None: # alt_text1은 무조건 None인 경우
            if len(alt_text2.strip()) > 0:
                alt_text = alt_text2
                print(f"[INFO] - {page} 존재하는 내용으로 가져왔어요:",alt_text)
                return alt_text
            # alt_text2 == None이 아니고 alt_text2의 길이가 0인경우(공백)
            else:
                return "정보없음"
    # 둘다 None 타입인 경우
    else:
        return "정보없음"
    
result_list = []
def get_data(api_key:str,**kwargs):
    import time
    import requests
    import pandas as pd
    """
    event table을 생성한다.
    서비스의 시작지점.

    args:
        api_key : 서울 공공데이터 api키

    push key:
        row_dataframe
        - parse_html 함수 호출로 event 테이블은 sync 테이블과 다르게 event_description 이 존재한다.
        - 단순히 호출된 데이터 이므로 row_dataframe으로 정의한다.
    """
    BATCH_DATE = kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d")
    print("[INFO] - get data from seoul open api")
    print("[INFO] - data source - seoul event data")
    print(f"[INFO] - Batch date - {BATCH_DATE}")
    

    try:
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/culturalEventInfo/1/1"
        first_response = requests.get(url, timeout=5)
        first_response.raise_for_status()

        json_data = first_response.json()
        # HTML PARSING
        address = json_data['culturalEventInfo']['row'][0]['HMPG_ADDR']
        description_str = parse_html(address,1)

        # dictionary 객체 참조하고 키값에 할당
        json_data['culturalEventInfo']['row'][0]['ALT'] = description_str

        result_list.extend(json_data['culturalEventInfo']['row'])
        
        # 요청page수
        end_page = json_data['culturalEventInfo']['list_total_count']
        print(f"[INFO] - request data amount : {end_page}")

    except requests.exceptions.RequestException as e:
        print("[EXCEPTION] - check your seoul api key or check OPEN API server..")
    
    # 페이지 수 만큼 api 요청
    for page in range(2, end_page+1):
        if page % 20 == 0:
            print(f"[INFO] - now request page : {page}/{end_page}.")
        for retry in range(1,4):
            try:
                url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/culturalEventInfo/{page}/{page}"
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    response = response.json()
                    data = response['culturalEventInfo']['row']
                    # HTML PARSING
                    address = data[0]["HMPG_ADDR"]
                    description_str = parse_html(address,page)
                    data[0]['ALT'] = description_str
                    if description_str == '정보없음':
                        print(f"[EXCEPTION] - {page} 페이지의 상세정보를 Parsing 하는데 실패했습니다. event_sync 테이블 확인요망.")
                    result_list.extend(data)
                    break

            except requests.exceptions.RequestException as e:
                print(f"[EXCEPTION] - make error in this page: {page} - {e}")
                print(f"[catch] - retry for four more times after 10 seconds .{retry}/4")
                time.sleep(10)
                continue

    # json to table
    df = pd.DataFrame(result_list)
    print(f"[INFO] - row_dataframe`s row: {len(df)}")
    # task instance
    ti = kwargs['ti']
    print("[INFO] - xcom_push - key : row_dataframe, value : dataframe")
    ti.xcom_push(key='row_dataframe', value=df)