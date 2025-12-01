import json
from pathlib import Path

class QueryLoader:
    """
    Usage:
        ql = QueryLoader("ticker_info.json")
        sql = ql.getQueryString("ticker_info")
    """

    def __init__(self, file_name: str):
        self.file_name = file_name
        
        # JSON 파일이 있는 query 폴더 지정
        # Airflow home 기준: news_stock_airflow/query
        # Docker 환경이면 /opt/airflow/query
        self.query_root = Path(__file__).resolve().parents[2] / "query"

        self.data = self._load_json()

    def _load_json(self):
        file_path = self.query_root / self.file_name
        print(f"[QueryLoader] Loading JSON from: {file_path}")  # 로그 확인

        if not file_path.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError(f"JSON must be a list of objects. File: {self.file_name}")

        return data

    def getQueryString(self, query_id: str):
        for item in self.data:
            if item.get("id") == query_id:
                return item.get("query").strip()
        raise KeyError(f"Query id '{query_id}' not found in {self.file_name}")