import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

def parse_html(html_page_url)->str:
    '''
    div 에서 행사정보를 paring 해서 리턴하는 함수

    culture-content 클래스의 alt 를 우선적으로 파싱하고,
    img-box 클래스의 alt를 두번째 순위로 파싱한다.
    '''

    response = requests.get(html_page_url)
    soup = BeautifulSoup(response.text,'html.parser')
    img_tag = None
    alt_text = None

    # culture-content의 내용확인
    culture_div = soup.find('div', class_='culture-content')
    if culture_div:
        img_tag = culture_div.find('img')
        if img_tag and img_tag.has_attr('alt'):
            alt_text = img_tag['alt']

    # culture-content의 내용이 없다면 다른 이미지 class 확인
    if not alt_text:
        img_box = soup.find('div', class_='img-box')
        if img_box:
            img_tag = img_box.find('img')
            if img_tag and img_tag.has_attr('alt'):
                alt_text = img_tag['alt']

    return alt_text if alt_text else '정보 없음'


result_list = []
def get_data(api_key:str,**kwargs):
    """
    전달받은 api_key 를 이용해 문화데이터를 수집하고 테이블을 생성하는 함수

    파싱한 값을 airflow task instance에 push한다.
    """
    BATCH_DATE = kwargs["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d")
    print(BATCH_DATE +"일자의 BATCH 처리를 시작합니다.")
    print("서울 문화행사 정보 요청")

    try:
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/culturalEventInfo/1/1"
        first_response = requests.get(url, timeout=5)
        first_response.raise_for_status()

        json_data = first_response.json()
        # HTML PARSING
        address = json_data['culturalEventInfo']['row'][0]['HMPG_ADDR']
        description_str = parse_html(address)

        # dictionary 객체 참조하고 키값에 할당
        json_data['culturalEventInfo']['row'][0]['ALT'] = description_str

        result_list.extend(json_data['culturalEventInfo']['row'])
        
        # 요청page수
        end_page = json_data['culturalEventInfo']['list_total_count']
         # 테스트용 상수 지울것!!!
        end_page = 60

        print(f"전체 데이터 건수: {end_page}")

    except requests.exceptions.RequestException as e:
        print("api_key를 확인해주세요. 혹은 API SERVER 자체 오류입니다.")
    
    # 페이지 수 만큼 api 요청
    for page in range(2, end_page+1):
        if page % 20 == 0:
            print(f"{page}/{end_page} 를 호출중입니다.")
        try:
            url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/culturalEventInfo/{page}/{page}"
            response = requests.get(url, timeout=5)
            if response.status_code != 200:
                print(f"[오류] 페이지 {page} - 상태 코드 {response.status_code}")
                break
            response = response.json()
            data = response['culturalEventInfo']['row']
            # HTML PARSING
            address = data[0]["HMPG_ADDR"]
            description_str = parse_html(address)
            data[0]['ALT'] = description_str
            if description_str == '정보없음':
                print(f"{page} 페이지의 상세정보를 Parsing 하는데 실패했습니다.")
            result_list.extend(data)

            # # 유효 일자 필터링
            # start_date = data[0]['STRTDATE']
            # end_date = data[0]['END_DATE']

            # # 아직 행사가 시작되지 않은 경우는 모두 수집 대상
            # if start_date >= BATCH_DATE:
            #     result_list.extend(data)
            #     continue
            
            # # 행사가 진행중인 경우
            # if (start_date <= BATCH_DATE)and(end_date >= BATCH_DATE):
            #     result_list.extend(data)
            #     continue

        except requests.exceptions.RequestException as e:
            print(f"[예외 발생] 페이지 {page} - {e}")
            continue

    # json to table
    df = pd.DataFrame(result_list)
    print(f"최종 수집 건수: {len(df)}")
    print(df)
    # task instance
    ti = kwargs['ti']
    ti.xcom_push(key='row_dataframe', value=df)

if __name__ == "__main__":
    # 테스트시 apikey, 오늘날짜 명시 필요
    pass