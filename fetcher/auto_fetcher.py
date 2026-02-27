import os
import json
import base64
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# API Endpoints
BASE_URL = "http://apis.data.go.kr/B551177/statusOfAllFltDeOdp"
DEPARTURE_URL = f"{BASE_URL}/getFltDeparturesDeOdp"
ARRIVAL_URL = f"{BASE_URL}/getFltArrivalsDeOdp"

def get_flight_data(endpoint, service_key, target_date):
    """특정 날짜의 운항 정보를 API로부터 수집합니다."""
    params = {
        'serviceKey': service_key,
        'searchdtCode': 'E',
        'searchDate': target_date,
        'searchFrom': '0000',
        'searchTo': '2400',
        'type': 'json',
        'numOfRows': '1',
        'pageNo': '1'
    }

    # 1. totalCount 확인
    response = requests.get(endpoint, params=params)
    response.raise_for_status()
    initial_data = response.json()
    
    try:
        total_count = initial_data['response']['body']['totalCount']
    except KeyError:
        return []

    if total_count == 0:
        return []

    # 2. 전체 데이터 요청
    params['numOfRows'] = str(total_count)
    full_response = requests.get(endpoint, params=params)
    full_response.raise_for_status()
    
    full_data = full_response.json()
    
    # [수정된 부분] 방어적 데이터 파싱 로직 적용
    body = full_data.get('response', {}).get('body', {})
    items = body.get('items', [])
    
    # 만약 items가 딕셔너리이고 그 안에 'item' 키가 있다면 한 꺼풀 벗겨냅니다.
    if isinstance(items, dict) and 'item' in items:
        items = items['item']
        
    # 결과가 리스트가 아니면(단일 객체일 경우) 리스트로 감싸서 반환합니다.
    return items if isinstance(items, list) else [items]

def upload_via_gas(file_name, json_data, gas_url):
    """데이터를 Base64로 인코딩하여 구글 앱스 스크립트 웹 앱으로 전송합니다."""
    # JSON 객체를 문자열로 변환 후 Base64 인코딩
    json_string = json.dumps(json_data, ensure_ascii=False)
    encoded_data = base64.b64encode(json_string.encode('utf-8')).decode('utf-8')

    payload = {
        "fileName": file_name,
        "mimeType": "application/json",
        "fileData": encoded_data
    }

    response = requests.post(gas_url, data=payload)
    print(f"드라이브 전송 결과: {response.text}")

def main():

    load_dotenv()
    service_key = os.getenv("IIAC_SERVICE_KEY")
    gas_url = os.getenv("GAS_WEB_APP_URL")

    if not service_key:
        raise ValueError("환경 변수(API 키)가 설정되지 않았습니다.")
    if not gas_url:
        raise ValueError("환경 변수(GAS URL)가 설정되지 않았습니다.")


    # 어제 날짜 계산 (KST 기준)
    yesterday = datetime.utcnow() + timedelta(hours=9) - timedelta(days=1)
    target_date = yesterday.strftime("%Y%m%d")

    print(f"데이터 수집 시작 날짜: {target_date}")

    departures = get_flight_data(DEPARTURE_URL, service_key, target_date)
    arrivals = get_flight_data(ARRIVAL_URL, service_key, target_date)

    daily_data = {
        "date": target_date,
        "departures_count": len(departures),
        "arrivals_count": len(arrivals),
        "departures": departures,
        "arrivals": arrivals
    }

    file_name = f"flights_{target_date}.json"
    upload_via_gas(file_name, daily_data, gas_url)

if __name__ == "__main__":
    main()