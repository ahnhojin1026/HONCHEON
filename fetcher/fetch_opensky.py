import requests
import struct
import json
import time
from datetime import datetime

# 1. 대상 공항 (진짜 ICAO 코드)
TARGET_AIRPORTS = ["RKSI", "KJFK", "EGLL", "LFPG", "RJTT"]
HOT_DATA_PATH = "../data/graph_data_real.bin"
COLD_DATA_PATH = "../data/edge_metadata_real.json"

def fetch_real_data():
    print("=== [HONCHEON] Real-world Data Ingestion Started ===")
    
    # 어제 하루치 데이터 요청 (Unix Timestamp)
    end_time = int(time.time()) - 3600 # 1시간 전까지
    begin_time = end_time - 86400      # 24시간 전부터
    
    nodes = []
    edges = []
    metadata = {}
    edge_id_counter = 0
    current_edge_offset = 0

    # 공항별 인덱스 매핑 (ID 보존용)
    airport_to_idx = {icao: i for i, icao in enumerate(TARGET_AIRPORTS)}

    for i, src_icao in enumerate(TARGET_AIRPORTS):
        print(f"\n[Requesting] Flights departing from {src_icao}...")
        url = f"https://opensky-network.org/api/flights/departure?airport={src_icao}&begin={begin_time}&end={end_time}"
        
        try:
            # 사지방 보안망/속도 고려하여 timeout 설정
            response = requests.get(url, timeout=15)
            flights = response.json()
            if not flights: flights = []
        except Exception as e:
            print(f"  [Error] Failed to fetch {src_icao}: {e} {response.status_code}")
            flights = []

        # 유효한 노선만 필터링 (도착지가 우리가 정한 5개 공항 중 하나인 경우만)
        valid_flights = [f for f in flights if f.get('estArrivalAirport') in airport_to_idx]
        
        src_id = int.from_bytes(src_icao.encode('ascii'), 'little')
        nodes.append((src_id, current_edge_offset, len(valid_flights)))

        for flight in valid_flights:
            arrival_icao = flight['estArrivalAirport']
            target_idx = airport_to_idx[arrival_icao]
            
            # 실제 비행 시간 계산 (초 단위 -> 분 단위)
            duration = (flight['lastSeen'] - flight['firstSeen']) // 60
            if duration <= 0: duration = 300 # 데이터 오류 시 기본값 5시간
            
            # --- Hot Data (16 Bytes) ---
            edges.append((target_idx, duration, edge_id_counter, 0))
            
            # --- Cold Data (실제 데이터 기록) ---
            metadata[f"edge_{edge_id_counter}"] = {
                "callsign": flight.get('callsign', 'UNKNOWN').strip(),
                "icao24": flight.get('icao24'),
                "aircraft_type": "REAL_DATA", # 오픈스카이는 무료 API에서 기종 제한적임
                "departure": src_icao,
                "arrival": arrival_icao,
                "real_duration_min": duration
            }
            print(f"  -> Real Edge {edge_id_counter}: {src_icao} to {arrival_icao} ({duration} min)")
            edge_id_counter += 1
            
        current_edge_offset += len(valid_flights)

    # 바이너리 및 JSON 저장 (아까와 동일한 로직)
    with open(HOT_DATA_PATH, "wb") as f:
        f.write(struct.pack('<II', len(nodes), len(edges)))
        for n in nodes: f.write(struct.pack('<III', n[0], n[1], n[2]))
        for e in edges: f.write(struct.pack('<IIII', e[0], e[1], e[2], e[3]))
    
    with open(COLD_DATA_PATH, "w", encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

    print(f"\n=== [Success] Real-world Data Packed: {len(edges)} Edges found. ===")

if __name__ == "__main__":
    fetch_real_data()