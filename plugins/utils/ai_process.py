import lancedb
from google import genai
import json
import pandas as pd
import numpy as np
from datetime import datetime
from sentence_transformers import SentenceTransformer
from langchain_community.embeddings import HuggingFaceEmbeddings

class AiProcess:
    def __init__(self, api_key, vector_db_path):
        self.api_key = api_key
        self.vector_db_path = vector_db_path
        self.client = genai.Client(api_key=api_key)
        self.model = "gemini-embedding-001"

    # -----------------------------
    # 1) LanceDB Insert / Update
    # -----------------------------
    def lancedb_create_or_update(self, vector_db_table, doc_list, pk):
        db = lancedb.connect(self.vector_db_path)

        if vector_db_table not in db.table_names():
            tbl = db.create_table(name=vector_db_table, data=doc_list)
            print(f"[LanceDB] 신규 테이블 생성 → {vector_db_table}")
            print(f"row count: {tbl.count_rows()}")
        else:
            tbl = db.open_table(vector_db_table)
            print(f"[LanceDB] 기존 테이블 업데이트 → {vector_db_table}")
            print(f"count before : {tbl.count_rows()}")

            tbl.merge_insert(pk) \
               .when_matched_update_all() \
               .when_not_matched_insert_all() \
               .execute(doc_list)

            print(f"count after : {tbl.count_rows()}")

        return tbl

    # -----------------------------
    # 2) Gemini Batch Embedding
    # -----------------------------
    def safe_gemini_batch_embedding(self, df):
        MAX_PAYLOAD_BYTES = 35 * 1024 * 1024

        batches = []
        current_batch = []
        current_size = 0

        # df → row 단위 배치 구성
        for idx, row in df.iterrows():
            text = f"{row['title']}\n{row['content']}"
            text_bytes = len(json.dumps(text).encode("utf-8"))

            if current_size + text_bytes > MAX_PAYLOAD_BYTES:
                batches.append(current_batch)
                current_batch = []
                current_size = 0

            current_batch.append((row, text))
            current_size += text_bytes

        if current_batch:
            batches.append(current_batch)

        print(f"[EMEBD BATCH NUM] : {len(batches)}")

        # 실제 Embedding 호출
        results = []

        for batch in batches:
            texts = [item[1] for item in batch]

            response = self.client.models.embed_content(
                model=self.model,
                contents=texts
            )

            embeddings = response.embeddings

            for i, (row, _) in enumerate(batch):
                results.append({
                    "seq": row["seq"],
                    "title": row["title"],
                    "content": row["content"],
                    "pubDate": row["pubDate"],
                    "media": row["media"],
                    "embedding": embeddings[i].values,
                    "insert_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

        return results
    
    # -----------------------------
    # 3) similarity check
    # -----------------------------
    def news_similarity_process(self, df_new_articles: pd.DataFrame, vector_db_table: str,search_dt:str):
        """
        새로 임베딩된 뉴스 목록을 기준으로 LanceDB 내에서 유사한 뉴스 기사를 검색합니다.
        
        Args:
            df_new_articles: 새로 임베딩된 뉴스 데이터프레임 (seq, title, content, ...)
            table_name: LanceDB 테이블 이름
            
        Returns:
            유사도 검색 결과 리스트 (list of dicts)
        """
        db = lancedb.connect(self.vector_db_path)
        tbl = db.open_table(vector_db_table)

        similarity_results = []

        # 각 뉴스 embedding 가져와 similarity search
        for _, row in df_new_articles.iterrows():
            seq = row["seq"]

            # LanceDB에서 해당 seq의 embedding 조회 (이미 이전 태스크에서 저장되었으므로 존재함)
            # 'where' 조건으로 특정 seq를 검색하고, limit(1)로 임베딩 벡터를 가져옵니다.
            query = tbl.search(None).where(f"seq = {seq}").limit(1).to_pandas()
            
            if query.empty:
                print(f"[WARN] seq={seq} embedding not found in LanceDB")
                continue
            
            emb = query.iloc[0]["embedding"]

            # top-10 유사 뉴스 검색 (default: Euclidean distance, or Cosine/Dot Product)
            # .search(emb)를 사용하면 LanceDB가 emb와 가장 가까운 벡터들을 찾아줍니다.
            topk = tbl.search(emb).limit(10)\
            .to_pandas()
                # .where(f"('pubDate' LIKE {search_dt})")\


            # 자기 자신 제거 및 유사도 점수 저장
            topk = topk[topk["seq"] != seq]

            for _, r in topk.iterrows():
                similarity_results.append({
                    "base_seq": seq,
                    "cmp_seq": r["seq"],
                    # _distance는 Distance Metric에 따라 Cosine Distance (0에 가까울수록 유사) 또는 Similarity Score를 의미
                    "similarity_score": float(r["_distance"]),     
                    "media": row["media"],
                })
            # print(pd.DataFrame(similarity_results))
                
        return pd.DataFrame(similarity_results).astype(str)
    
    def df_to_sql_values_string(self, df: pd.DataFrame) -> str:
        """
        DataFrame → SQL VALUES 문자열 생성
        숫자는 그대로, 문자열은 quotes 처리
        """
        def escape_sql(val):
            if pd.isna(val) or val is None:
                return "NULL"
            if isinstance(val, str):
                return "'" + val.replace("'", "''") + "'"  # SQL 문자열 escape
            return str(val)  # 숫자는 그대로

        values_str_list = []

        for _, row in df.iterrows():
            escaped_values = [escape_sql(row[col]) for col in df.columns]
            tuple_str = "(" + ", ".join(escaped_values) + ")"
            values_str_list.append(tuple_str)

        return ", ".join(values_str_list)
    


    # -----------------------------
    # 2) Local (ko-sentence-transformers) Batch Embedding
    # -----------------------------
    def safe_local_batch_embedding(self, df):
        MAX_BATCH_SIZE = 50
        # 모델 로드 (한번만 로드)
        embedding_model = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        print(f"[EMBEDDING MODEL] all-MiniLM-L6-v2")
        print(f"[INPUT SIZE] df={len(df)} rows")

        results = []

        for i in range(0, len(df), MAX_BATCH_SIZE):
            batch = df.iloc[i:i + MAX_BATCH_SIZE]

            # 텍스트 리스트 만들기
            texts = [
                f"{row['title']}\n{row['content']}"
                for _, row in batch.iterrows()
            ]

            # 🔥 HuggingFace 임베딩
            embeddings = embedding_model.embed_documents(texts)

            # 결과 append
            for emb, (_, row) in zip(embeddings, batch.iterrows()):
                results.append({
                    "seq": row["seq"],
                    "title": row["title"],
                    "content": row["content"],
                    "pubDate": row["pubDate"],
                    "media": row["media"],
                    "embedding": emb,
                    "insert_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

        print(f"[EMBED RESULT] rows={len(results)}")
        return results