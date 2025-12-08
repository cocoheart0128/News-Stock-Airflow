import lancedb
from google import genai
import json
import pandas as pd
import numpy as np
from datetime import datetime
from sentence_transformers import SentenceTransformer
from langchain_community.embeddings import HuggingFaceEmbeddings
from pydantic import BaseModel

class NewsSummary(BaseModel):
    # analysis_type: str 
    related_keyword: str
    summary_content: str
    keyword_summary_content: str
    general_sentiment :str
    general_score : float
    general_content : str  ## 감정 근거 설명
    industry_sentiment : str 
    industry_score : float 
    industry_content : str 
    keyword_criteria : str 
    fit_score : float 
    fit_comment : str


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
            topk_all = tbl.search(emb).limit(100).to_pandas()
            topk = topk_all[topk_all['pubDate'].str.startswith(search_dt)].head(11)
            # topk = topk_all[topk_all['pubDate'].str.contains(f"{search_dt}%", regex=True)].head(11)
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
    

    def gemini_ai_summary(self,news_title,news_content):
        # ----------------------------
        # 프롬프트 (JSON 구조 요청)
        # ----------------------------
        prompt = f"""
        다음 뉴스 기사 내용을 분석하여 아래 JSON 형식 그대로 출력하라. 
        반드시 JSON만 출력하고 다른 설명은 절대 포함하지 마라.
        산업분야 키워드에 대해서 아래와 같은 구조로 되어 있어야하고 아래 키워드말고 추가적으로 해도 된다.
        대분류(키워드1),중분류(키워드2),소분류(키워드3)
        재무·실적,실적 발표,매출
        재무·실적,실적 발표,영업이익
        재무·실적,실적 발표,순이익
        신사업·R&D,기술 개발,AI
        신사업·R&D,기술 개발,반도체


        [필수 규칙]
        1) related_keyword:
        - 뉴스 산업 분야 3단 키워드
        - 반환 포맷: '키워드1 > 키워드2 > 키워드3'

        2) summary_content: 
        - 뉴스 3단 요약 생성.
        - 각 단락은 줄바꿈(\n)으로 구분.
        - 각 문단 앞에 '· [핵심문구N]' 레이블 붙이기.
        - [핵심문구] 조건:
            - 10글자 이상 20글자 미만
            - 각 문단별 핵심 요약 키워드
        - 전체적으로 사실 기반 요약일 것.

        3) keyword_summary_content (산업 분야 3단 키워드):
        - 위에 1번 related_keyword 기반한 요약
        - 뉴스 1단 요약 생성.
        - 뉴스 산업 분야 주요 계층 키워드로 구성할 것.

        4) general_sentiment:
        - 뉴스가 **해당 산업/기업 주가**에 미치는 영향을 기준으로 분류
        - "긍정" / "부정" / "중립" 중 하나만 출력

        5) general_score:
        - -1.00 ~ 1.00 사이 float
        - 부정적일수록 -1.00 과 가까움
        - 긍정적일수록 1.00 과 가까움
        - 중립이면 0.00 근처

        6) general_content:
        - 감정 판단의 핵심 근거 2~3문장

        7) industry_sentiment:
        - 산업 전반(IT/금융/자동차/모빌리티 등)에 미치는 영향
        - "긍정" / "부정" / "중립"

        8) industry_score:
        - -1.00 ~ 1.00

        9) industry_content:
        - 산업 영향 판단 근거 2~3문장

        10) keyword_criteria:
        - keyword_summary_content의 3단 키워드 각각을 선택한 이유 설명

        11) fit_score:
        - 해당 뉴스가 특정 기업 또는 산업 분석에 얼마나 적합한지 판단
        - 0~1 스코어 (1이 가장 적합)

        12) fit_comment:
        - fit_score에 대한 2~3문장 설명

        --------------------------------
        출력 JSON 스키마 (이 형태 그대로 출력)
        --------------------------------

{{
  "related_keyword": "재무·실적 > 실적 발표 > 주가평가",
  "summary_content": "· [AI거품경고확산] 마이클 버리가 AI 거품론을 이어 테슬라 역시 펀더멘털 대비 과도하게 고평가됐다고 주장하며 시장 불안 심리를 자극했다.\n· [주주가치희석문제] 그는 테슬라가 잦은 주식 분할·신주 발행·주식보상 정책으로 주주가치를 매년 3% 이상 희석해 왔다고 지적했다.\n· [투자자신뢰논란] 버리의 잇단 경고에도 그의 투자 성과와 운용자산 감소가 논란이 되며 시장 내 신뢰성에 대한 평가가 엇갈리고 있다.",
  "keyword_summary_content": "재무·실적 관점에서 테슬라의 주가가 주주가치 희석 요인으로 인해 고평가됐다는 평가가 제기됐다.",
  "general_sentiment": "부정",
  "general_score": -0.62,
  "general_content": "테슬라의 주가가 펀더멘털 대비 과도하게 높고 주주가치가 지속적으로 희석되고 있다는 지적은 투자 심리에 부정적으로 작용한다. 또한 CEO 보상 정책과 신주 발행 이슈는 중장기 성장성에 대한 의구심을 강화할 수 있다.",
  "industry_sentiment": "부정",
  "industry_score": -0.35,
  "industry_content": "AI·전기차 섹터 전반에 대한 고평가 논란이 확산되며 투자자들의 밸류에이션 경계심리가 커질 수 있다. 특히 고성장주 중심의 시장에서는 변동성 확대 요인으로 작용할 가능성이 있다.",
  "keyword_criteria": "재무·실적을 선택한 이유는 핵심 논지가 테슬라의 주가·밸류에이션과 주주가치 희석 문제이기 때문이다. 실적 발표 계층을 선택한 이유는 주식 보상, 신주 발행 등 기업의 재무적 활동이 주가 평가 논쟁의 중심이기 때문이다. 소분류는 주가 평가 및 가치 희석이라는 본질적 이슈에 따라 설정했다.",
  "fit_score": 0.78,
  "fit_comment": "테슬라의 주가·밸류에이션 문제를 중심으로 한 분석이므로 산업·기업 분석에 상당히 적합하다. 다만 실적 수치 중심의 재무 데이터보다는 평가 이슈 중심이라 100% 정량적 분석에는 제한이 있다."
}}


        --------------------------------
        아래는 분석 대상 뉴스 제목이다:
        --------------------------------
        {news_title}

        --------------------------------
        아래는 분석 대상 뉴스 본문이다:
        --------------------------------
        {news_content}

        """

        # ----------------------------
        # 5️⃣ 모델 호출
        # ----------------------------
        response = self.client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_schema": NewsSummary,
            }
        )

        return response.parsed