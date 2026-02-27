import os
import requests
import struct
import json
from dotenv import load_dotenv

load_dotenv()
SERVICE_KEY = os.getenv("IIAC_SERVICE_KEY")

HOT_DATA_PATH = "../data/graph_data.bin"
COLD_DATA_PATH = "../data/edge_metadata.json"

def fetch_iiac_departures(num_rows=100):
    url = "http://apis.data.go.kr/B551177/statusOfAllFltDeOdp/getFltDeparturesDeOdp"
    # 공공데이터포털은 종종 Decoding된 키를 요구합니다.
    params = {'serviceKey': SERVICE_KEY, 'type': 'json', 'numOfRows': num_rows, 'pageNo': 1}
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code != 200:
            print(f"[!] HTTP Error: {response.status_code}")
            return []
        
        data = response.json()
        items = data.get('response', {}).get('body', {}).get('items', [])
        
        # items가 딕셔너리 하나인 경우(결과가 1개일 때)를 대비해 리스트로 변환
        if isinstance(items, dict):
            items = [items]
            
        return items
    except Exception as e:
        print(f"[!] Fetch Exception: {e}")
        return []

def build_honcheon_graph():
    raw_data = fetch_iiac_departures(num_rows=1000)
    if not raw_data: 
        print("[!] No data received from API.")
        return

    # 디버깅: 첫 번째 데이터의 실제 키 구조 확인
    sample = raw_data[0]
    print(f"[*] Raw data keys found in API: {list(sample.keys())}")
    print(f"[*] Sample Item: {sample}")

    src_node_name = "ICN"
    src_node_id = int.from_bytes(src_node_name.encode(), 'little')
    
    physical_flights = {}
    
    for item in raw_data:
        # 다양한 키 후보군 체크 (API 버전마다 다를 수 있음)
        sched_time = item.get('scheduleDateTime') or item.get('scheduledDateTime') or item.get('scheduleDatetime')
        dest_code = item.get('airportCode') or item.get('airport')
        
        if not sched_time or not dest_code: 
            continue
        
        key = (sched_time, dest_code)
        if key not in physical_flights:
            physical_flights[key] = []
        physical_flights[key].append(item)

    print(f"[*] Grouped {len(raw_data)} records into {len(physical_flights)} physical flights.")

    if not physical_flights:
        print("[!] Grouping failed. Please check the 'Raw data keys' above and match them.")
        return

    unique_airports = list(set([k[1] for k in physical_flights.keys()]))
    airport_map = {code: i + 1 for i, code in enumerate(unique_airports)}
    airport_map[src_node_name] = 0
    
    nodes, edges, metadata = [], [], {}
    
    current_offset = 0
    for i, (key, flight_group) in enumerate(physical_flights.items()):
        sched_time, target_code = key
        target_idx = airport_map[target_code]
        
        master = flight_group[0]
        codeshares = [f.get('flightId') for f in flight_group[1:]]
        
        edge_id = i
        # Hot Data: 물리적 엣지 1개
        edges.append((target_idx, 100, edge_id, 0))
        
        # Cold Data
        metadata[f"edge_{edge_id}"] = {
            "operating_flight": master.get('flightId'),
            "codeshares": codeshares,
            "airline": master.get('airline'),
            "destination": target_code,
            "schedule_time": sched_time,
            "is_codeshare": len(flight_group) > 1
        }
    
    nodes.append((src_node_id, 0, len(edges)))
    for code, idx in airport_map.items():
        if code == src_node_name: continue
        nodes.append((int.from_bytes(code.encode(), 'little'), 0, 0))

    with open(HOT_DATA_PATH, "wb") as f:
        f.write(struct.pack('<II', len(nodes), len(edges)))
        for n in nodes: f.write(struct.pack('<III', n[0], n[1], n[2]))
        for e in edges: f.write(struct.pack('<IIII', e[0], e[1], e[2], e[3]))

    with open(COLD_DATA_PATH, "w", encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"[+] Success! '{HOT_DATA_PATH}' generated with {len(edges)} physical edges.")

if __name__ == "__main__":
    if SERVICE_KEY: build_honcheon_graph()