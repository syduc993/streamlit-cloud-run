import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import os
import math
import requests
from typing import Dict, List, Optional
from math import ceil

# Cấu hình trang
st.set_page_config(
    page_title="BigQuery to Larkbase",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS để ẩn sidebar hoàn toàn
st.markdown("""
<style>
    [data-testid="stSidebar"] {display: none;}
    [data-testid="collapsedControl"] {display: none;}
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 100%;
    }
    .stTextArea textarea {
        height: 120px;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 0.5rem;
        border-radius: 0.5rem;
        text-align: center;
        margin: 0.25rem 0;
    }
    .larkbase-section {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Larkbase Configuration
class LarkbaseConfig:
    def __init__(self, app_id=None, app_secret=None, api_endpoint=None):
        self.app_id = app_id or 'cli_a7fab27260385010'
        self.app_secret = app_secret or 'Zg4MVcFfiOu0g09voTcpfd4WGDpA0Ly5'
        self.api_endpoint = api_endpoint or 'https://open.larksuite.com/open-apis'
    
    def to_dict(self) -> Dict:
        return {
            'app_id': self.app_id,
            'app_secret': self.app_secret,
            'api_endpoint': self.api_endpoint
        }

class LarkbaseAuthenticator:
    def __init__(self, config: LarkbaseConfig):
        self.config = config
    
    def authenticate(self) -> Optional[str]:
        """Xác thực với API Larkbase để lấy access token"""
        try:
            url = f"{self.config.api_endpoint}/auth/v3/tenant_access_token/internal"
            response = requests.post(url, json={
                'app_id': self.config.app_id, 
                'app_secret': self.config.app_secret
            })
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == 0:
                return data.get('tenant_access_token')
            else:
                st.error(f"Lỗi API Larkbase: {data.get('msg', 'Không xác định')}")
                return None
        except Exception as e:
            st.error(f"Lỗi xác thực Larkbase: {str(e)}")
            return None

class LarkbaseRecordManager:
    def __init__(self, access_token: str, config: LarkbaseConfig):
        self.access_token = access_token
        self.config = config
    
    def get_all_records(self, app_token: str, table_id: str) -> List[str]:
        """Lấy tất cả record IDs từ bảng"""
        url = f"{self.config.api_endpoint}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        all_record_ids = []
        page_token = None
        
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            
            response = requests.get(url, headers=headers, params=params)
            
            try:
                data = response.json()
                if data.get('code') == 0:
                    records = data.get('data', {}).get('items', [])
                    record_ids = [record.get('record_id') for record in records]
                    all_record_ids.extend(record_ids)
                    
                    # Kiểm tra có trang tiếp theo không
                    page_token = data.get('data', {}).get('page_token')
                    if not page_token:
                        break
                else:
                    st.error(f"Lỗi lấy records: {data.get('msg')}")
                    break
            except Exception as e:
                st.error(f"Lỗi parse response: {str(e)}")
                break
        
        return all_record_ids
    
    def batch_delete_records(self, records: List[str], app_token: str, table_id: str) -> Dict:
        """Xóa nhiều record khỏi bảng trên Lark Bitable"""
        if not records:
            return {"status": "no_records", "message": "Không có record nào để xóa."}

        url = f"{self.config.api_endpoint}/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        batch_size = 500
        total_records = len(records)
        total_batches = ceil(total_records / batch_size)
        results = []
        errors = []

        progress_bar = st.progress(0)
        status_text = st.empty()

        for i in range(total_batches):
            batch = records[i * batch_size:(i + 1) * batch_size]
            data = {"records": batch}
            response = requests.post(url, headers=headers, json=data)
            
            try:
                result = response.json()
                results.append(result)
                if response.status_code != 200 or result.get("code", 0) != 0:
                    errors.append({
                        "batch_index": i,
                        "status_code": response.status_code,
                        "response": result
                    })
                    status_text.text(f"Batch {i+1}/{total_batches}: Lỗi xóa {len(batch)} bản ghi")
                else:
                    status_text.text(f"Batch {i+1}/{total_batches}: Xóa thành công {len(batch)} bản ghi")
            except Exception as e:
                errors.append({
                    "batch_index": i,
                    "status_code": response.status_code,
                    "exception": str(e)
                })
                status_text.text(f"Batch {i+1}/{total_batches}: Lỗi xóa")
            
            progress_bar.progress((i + 1) / total_batches)

        summary = {
            "total_batches": total_batches,
            "total_records": total_records,
            "success_batches": total_batches - len(errors),
            "error_batches": len(errors),
            "results": results,
            "errors": errors
        }

        return summary
    
    def batch_create_records(self, records: List[Dict], app_token: str, table_id: str, batch_size: int = 500) -> List[Dict]:
        """Tạo nhiều record mới trong bảng trên Lark Bitable"""
        if not records:
            return [{"status": "no_records", "message": "Không có record nào để tạo."}]

        url = f"{self.config.api_endpoint}/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        total_records = len(records)
        total_batches = ceil(total_records / batch_size)
        results = []

        progress_bar = st.progress(0)
        status_text = st.empty()

        for i in range(0, total_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_records)
            batch = records[start_idx:end_idx]
            
            # Chuẩn bị dữ liệu cho API Larkbase
            formatted_batch = []
            for record in batch:
                formatted_record = {"fields": {}}
                for key, value in record.items():
                    # Chuyển đổi giá trị thành format phù hợp với Larkbase
                    if pd.isna(value):
                        formatted_record["fields"][key] = ""
                    elif isinstance(value, (int, float)):
                        formatted_record["fields"][key] = value
                    else:
                        formatted_record["fields"][key] = str(value)
                formatted_batch.append(formatted_record)
            
            data = {"records": formatted_batch}
            response = requests.post(url, headers=headers, json=data)
            
            try:
                res_json = response.json()
                if res_json.get('code') == 0:
                    result = {
                        "status": "success", 
                        "batch": i+1, 
                        "created_count": len(res_json['data']['records'])
                    }
                    status_text.text(f"Batch {i+1}/{total_batches}: Tạo thành công {len(batch)} bản ghi")
                else:
                    result = {
                        "status": "error", 
                        "batch": i+1, 
                        "msg": res_json.get('msg'), 
                        "code": res_json.get('code')
                    }
                    status_text.text(f"Batch {i+1}/{total_batches}: Lỗi - {res_json.get('msg')}")
            except Exception as e:
                result = {
                    "status": "error", 
                    "batch": i+1, 
                    "status_code": response.status_code, 
                    "exception": str(e)
                }
                status_text.text(f"Batch {i+1}/{total_batches}: Lỗi - {str(e)}")
            
            results.append(result)
            progress_bar.progress((i + 1) / total_batches)

        return results

# BigQuery functions (giữ nguyên như cũ)
@st.cache_resource
def init_bigquery_client():
    """Khởi tạo BigQuery client"""
    try:
        if os.getenv('K_SERVICE'):
            from google.auth import default
            credentials, project = default()
            return bigquery.Client(credentials=credentials, project=project)
        else:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=credentials)
    except Exception as e:
        st.error(f"❌ Lỗi kết nối BigQuery: {e}")
        return None

@st.cache_data(ttl=300)
def run_bigquery_query(query, limit=1000):
    """Thực thi query và trả về kết quả dạng DataFrame"""
    client = init_bigquery_client()
    if client is None:
        return None
    
    try:
        query_upper = query.upper().strip()
        if not 'LIMIT' in query_upper:
            query = f"{query.rstrip(';')} LIMIT {limit}"
        
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=100 * 1024 * 1024,  # 100MB limit
            use_query_cache=True
        )
        
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        st.error(f"❌ Lỗi thực thi query: {e}")
        return None

def validate_query(query):
    """Kiểm tra tính hợp lệ của SQL query"""
    dangerous_keywords = ['DELETE', 'DROP', 'TRUNCATE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
    query_upper = query.upper()
    
    for keyword in dangerous_keywords:
        if keyword in query_upper:
            return False, f"Query chứa từ khóa nguy hiểm: {keyword}"
    
    if not query.strip().upper().startswith('SELECT'):
        return False, "Query phải bắt đầu bằng SELECT"
    
    return True, "Query hợp lệ"

def paginate_dataframe(df, page_size=10):
    """Chia DataFrame thành các trang"""
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 0
    
    total_pages = math.ceil(len(df) / page_size)
    
    if total_pages > 1:
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        
        with col1:
            if st.button("⏮️ Đầu", disabled=st.session_state.current_page == 0):
                st.session_state.current_page = 0
                st.rerun()
        
        with col2:
            if st.button("◀️ Trước", disabled=st.session_state.current_page == 0):
                st.session_state.current_page -= 1
                st.rerun()
        
        with col3:
            st.markdown(f"<div class='metric-container'>Trang {st.session_state.current_page + 1} / {total_pages}</div>", 
                       unsafe_allow_html=True)
        
        with col4:
            if st.button("▶️ Sau", disabled=st.session_state.current_page >= total_pages - 1):
                st.session_state.current_page += 1
                st.rerun()
        
        with col5:
            if st.button("⏭️ Cuối", disabled=st.session_state.current_page >= total_pages - 1):
                st.session_state.current_page = total_pages - 1
                st.rerun()
    
    start_idx = st.session_state.current_page * page_size
    end_idx = start_idx + page_size
    return df.iloc[start_idx:end_idx]

def main():
    st.markdown("### 📊 BigQuery to Larkbase")
    
    # Kiểm tra kết nối BigQuery
    client = init_bigquery_client()
    if client is None:
        st.error("❌ Không thể kết nối đến BigQuery")
        st.stop()
    
    # Query section (giữ nguyên như cũ)
    col1, col2 = st.columns([4, 1])
    
    with col1:
        query = st.text_area(
            "SQL Query:",
            placeholder="SELECT * FROM `project.dataset.table` WHERE condition = 'value'",
            help="Chỉ hỗ trợ SELECT queries"
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        execute_button = st.button("🚀 Thực thi", type="primary", use_container_width=True)
        
        if query.strip():
            is_valid, message = validate_query(query)
            if is_valid:
                st.success("✅ Hợp lệ")
            else:
                st.error("❌ Không hợp lệ")
    
    # Execute query (giữ nguyên như cũ)
    if execute_button and query.strip():
        is_valid, message = validate_query(query)
        if not is_valid:
            st.error(f"❌ {message}")
            return
        
        with st.spinner("🔄 Đang truy vấn..."):
            df = run_bigquery_query(query)
            
            if df is not None and not df.empty:
                st.session_state.current_page = 0
                st.session_state.query_result = df
                
                # Metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("📊 Dòng", f"{len(df):,}")
                with col2:
                    st.metric("📋 Cột", len(df.columns))
                with col3:
                    memory_mb = df.memory_usage(deep=True).sum() / 1024**2
                    st.metric("💾 MB", f"{memory_mb:.1f}")
                with col4:
                    st.metric("📄 Trang", math.ceil(len(df) / 10))
                
                # Data display
                st.markdown("**📋 Kết quả:**")
                page_data = paginate_dataframe(df, page_size=10)
                st.dataframe(page_data, use_container_width=True, height=350)
                
                # Download options
                col1, col2 = st.columns(2)
                with col1:
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "📥 CSV",
                        csv,
                        f"data_{pd.Timestamp.now().strftime('%H%M%S')}.csv",
                        "text/csv",
                        use_container_width=True
                    )
                with col2:
                    json_data = df.to_json(orient='records', indent=2)
                    st.download_button(
                        "📥 JSON", 
                        json_data,
                        f"data_{pd.Timestamp.now().strftime('%H%M%S')}.json",
                        "application/json",
                        use_container_width=True
                    )
                
            elif df is not None:
                st.warning("⚠️ Query không trả về dữ liệu")
            else:
                st.error("❌ Lỗi thực thi query")
    
    # Larkbase section với tùy chọn xóa dữ liệu cũ
    if 'query_result' in st.session_state and not st.session_state.query_result.empty:
        st.markdown('<div class="larkbase-section">', unsafe_allow_html=True)
        st.markdown("### 📝 Ghi dữ liệu vào Larkbase")
        
        col1, col2 = st.columns(2)
        with col1:
            app_token = st.text_input(
                "App Token:",
                placeholder="bascnCMII2eTqzxI8qI5lc...",
                help="App Token của Larkbase"
            )
        with col2:
            table_id = st.text_input(
                "Table ID:",
                placeholder="tblxxx...",
                help="ID của bảng trong Larkbase"
            )
        
        # Tùy chọn xóa dữ liệu cũ
        clear_old_data = st.checkbox(
            "🗑️ Xóa tất cả dữ liệu cũ trước khi ghi mới",
            value=True,
            help="Nếu chọn, sẽ xóa toàn bộ dữ liệu hiện có trong bảng trước khi ghi dữ liệu mới"
        )
        
        if st.button("📤 Ghi vào Larkbase", type="secondary", use_container_width=True):
            if not app_token or not table_id:
                st.error("❌ Vui lòng nhập đầy đủ App Token và Table ID")
                return
            
            # Khởi tạo Larkbase
            config = LarkbaseConfig()
            authenticator = LarkbaseAuthenticator(config)
            
            with st.spinner("🔐 Đang xác thực Larkbase..."):
                access_token = authenticator.authenticate()
            
            if access_token:
                st.success("✅ Xác thực Larkbase thành công")
                record_manager = LarkbaseRecordManager(access_token, config)
                
                # Xóa dữ liệu cũ nếu được chọn
                if clear_old_data:
                    with st.spinner("🗑️ Đang lấy danh sách records cũ..."):
                        old_record_ids = record_manager.get_all_records(app_token, table_id)
                    
                    if old_record_ids:
                        st.info(f"📋 Tìm thấy {len(old_record_ids)} bản ghi cũ")
                        with st.spinner("🗑️ Đang xóa dữ liệu cũ..."):
                            delete_result = record_manager.batch_delete_records(old_record_ids, app_token, table_id)
                        
                        if delete_result.get("error_batches", 0) == 0:
                            st.success(f"✅ Đã xóa thành công {len(old_record_ids)} bản ghi cũ")
                        else:
                            st.warning(f"⚠️ Xóa hoàn tất với {delete_result.get('error_batches', 0)} lỗi")
                    else:
                        st.info("📋 Không có dữ liệu cũ để xóa")
                
                # Ghi dữ liệu mới
                records = st.session_state.query_result.to_dict('records')
                
                with st.spinner("📝 Đang ghi dữ liệu mới vào Larkbase..."):
                    results = record_manager.batch_create_records(records, app_token, table_id)
                
                # Hiển thị kết quả
                success_count = sum(1 for r in results if r.get("status") == "success")
                error_count = len(results) - success_count
                
                if error_count == 0:
                    st.success(f"✅ Đã ghi thành công {len(records)} bản ghi vào Larkbase!")
                else:
                    st.warning(f"⚠️ Ghi hoàn tất: {success_count} thành công, {error_count} lỗi")
                    
                    # Hiển thị chi tiết lỗi
                    errors = [r for r in results if r.get("status") == "error"]
                    if errors:
                        with st.expander("Chi tiết lỗi"):
                            for error in errors:
                                st.error(f"Batch {error.get('batch')}: {error.get('msg', error.get('exception'))}")
            else:
                st.error("❌ Không thể xác thực với Larkbase")
        
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()