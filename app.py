import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import os
import math

# Cấu hình trang - loại bỏ sidebar
st.set_page_config(
    page_title="BigQuery Reader",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS để ẩn sidebar hoàn toàn và tối ưu layout
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
</style>
""", unsafe_allow_html=True)

# Khởi tạo BigQuery client
@st.cache_resource
def init_bigquery_client():
    """Khởi tạo BigQuery client với service account"""
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

# Cache query results
@st.cache_data(ttl=300)
def run_bigquery_query(query, limit=250):
    """Thực thi query và trả về kết quả dạng DataFrame"""
    client = init_bigquery_client()
    if client is None:
        return None
    
    try:
        # Thêm LIMIT vào query nếu chưa có
        query_upper = query.upper().strip()
        if not 'LIMIT' in query_upper:
            query = f"{query.rstrip(';')} LIMIT {limit}"
        
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=50 * 1024 * 1024,  # 50MB limit
            use_query_cache=True
        )
        
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        st.error(f"❌ Lỗi thực thi query: {e}")
        return None

# Validate SQL query
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

# Pagination component
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
    
    # Hiển thị dữ liệu của trang hiện tại
    start_idx = st.session_state.current_page * page_size
    end_idx = start_idx + page_size
    return df.iloc[start_idx:end_idx]

def main():
    # Header compact
    st.markdown("### 📊 BigQuery Data Reader")
    
    # Kiểm tra kết nối
    client = init_bigquery_client()
    if client is None:
        st.error("❌ Không thể kết nối đến BigQuery")
        st.stop()
    
    # Query input area
    col1, col2 = st.columns([4, 1])
    
    with col1:
        query = st.text_area(
            "SQL Query:",
            placeholder="SELECT * FROM `project.dataset.table` WHERE condition = 'value'",
            help="Chỉ hỗ trợ SELECT queries"
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)  # Spacing
        execute_button = st.button("🚀 Thực thi", type="primary", use_container_width=True)
        
        if query.strip():
            is_valid, message = validate_query(query)
            if is_valid:
                st.success("✅ Hợp lệ")
            else:
                st.error("❌ Không hợp lệ")
    
    # Execute query
    if execute_button and query.strip():
        is_valid, message = validate_query(query)
        if not is_valid:
            st.error(f"❌ {message}")
            return
        
        with st.spinner("🔄 Đang truy vấn..."):
            df = run_bigquery_query(query)
            
            if df is not None and not df.empty:
                # Reset pagination khi có query mới
                st.session_state.current_page = 0
                
                # Metrics compact
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
                
                # Data display with pagination
                st.markdown("**📋 Kết quả:**")
                
                # Hiển thị dữ liệu với phân trang
                page_data = paginate_dataframe(df, page_size=10)
                st.dataframe(page_data, use_container_width=True, height=350)
                
                # Download options compact
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

if __name__ == "__main__":
    main()
