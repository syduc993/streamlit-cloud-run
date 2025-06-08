import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os

# Cấu hình trang
st.set_page_config(
    page_title="BigQuery Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS tùy chỉnh
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #1f77b4;
    }
    .stAlert > div {
        padding-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Khởi tạo BigQuery client
@st.cache_resource
def init_bigquery_client():
    """Khởi tạo BigQuery client với service account"""
    try:
        # Kiểm tra môi trường Cloud Run
        if os.getenv('K_SERVICE'):
            # Chạy trên Cloud Run - sử dụng default credentials
            from google.auth import default
            credentials, project = default()
            return bigquery.Client(credentials=credentials, project=project)
        else:
            # Chạy local - sử dụng secrets
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=credentials)
    except Exception as e:
        st.error(f"❌ Lỗi kết nối BigQuery: {e}")
        return None

# Cache query results
@st.cache_data(ttl=300)
def run_bigquery_query(query, max_results=10000):
    """Thực thi query và trả về kết quả dạng DataFrame"""
    client = init_bigquery_client()
    if client is None:
        return None
    
    try:
        # Cấu hình job
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=100 * 1024 * 1024,  # 100MB limit
            use_query_cache=True,
            dry_run=False
        )
        
        # Thực thi query
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        
        # Giới hạn số dòng trả về
        if len(df) > max_results:
            st.warning(f"⚠️ Kết quả có {len(df)} dòng, chỉ hiển thị {max_results} dòng đầu")
            df = df.head(max_results)
            
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

# Sample queries
def get_sample_queries():
    """Trả về danh sách các query mẫu"""
    return {
        "📚 Shakespeare Words": {
            "query": """
                SELECT 
                    word, 
                    word_count,
                    corpus
                FROM `bigquery-public-data.samples.shakespeare` 
                WHERE word_count > 100
                ORDER BY word_count DESC 
                LIMIT 50
            """,
            "description": "Từ vựng phổ biến trong tác phẩm Shakespeare"
        },
        "👥 USA Names Trends": {
            "query": """
                SELECT 
                    name,
                    gender,
                    year,
                    SUM(number) as total_births
                FROM `bigquery-public-data.usa_names.usa_1910_2013`
                WHERE year >= 2000
                GROUP BY name, gender, year
                HAVING total_births > 1000
                ORDER BY year DESC, total_births DESC
                LIMIT 100
            """,
            "description": "Xu hướng đặt tên ở Mỹ từ năm 2000"
        },
        "🌍 GitHub Languages": {
            "query": """
                SELECT 
                    language,
                    COUNT(*) as repo_count,
                    AVG(watch_count) as avg_watches,
                    SUM(watch_count) as total_watches
                FROM `bigquery-public-data.github_repos.sample_repos`
                WHERE language IS NOT NULL
                GROUP BY language
                HAVING repo_count > 10
                ORDER BY total_watches DESC
                LIMIT 20
            """,
            "description": "Ngôn ngữ lập trình phổ biến trên GitHub"
        },
        "🚲 London Bike Sharing": {
            "query": """
                SELECT 
                    EXTRACT(HOUR FROM start_date) as hour,
                    EXTRACT(DAYOFWEEK FROM start_date) as day_of_week,
                    COUNT(*) as trip_count,
                    AVG(duration) as avg_duration
                FROM `bigquery-public-data.london_bicycles.cycle_hire`
                WHERE start_date >= '2017-01-01'
                GROUP BY hour, day_of_week
                ORDER BY hour, day_of_week
                LIMIT 200
            """,
            "description": "Phân tích thuê xe đạp London theo giờ và ngày"
        }
    }

# Visualization functions
def create_bar_chart(df, x_col, y_col, title):
    """Tạo biểu đồ cột"""
    fig = px.bar(df.head(20), x=x_col, y=y_col, title=title)
    fig.update_layout(xaxis_tickangle=-45)
    return fig

def create_line_chart(df, x_col, y_col, title):
    """Tạo biểu đồ đường"""
    fig = px.line(df, x=x_col, y=y_col, title=title)
    return fig

def create_scatter_plot(df, x_col, y_col, title, color_col=None):
    """Tạo biểu đồ scatter"""
    fig = px.scatter(df, x=x_col, y=y_col, title=title, color=color_col)
    return fig

# Main application
def main():
    # Header
    st.markdown('<h1 class="main-header">📊 BigQuery Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Kiểm tra kết nối
    client = init_bigquery_client()
    if client is None:
        st.error("❌ Không thể kết nối đến BigQuery. Vui lòng kiểm tra cấu hình.")
        st.stop()
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Cấu hình Query")
        
        # Chọn loại query
        query_mode = st.radio(
            "Chọn chế độ:",
            ["🎯 Query mẫu", "✏️ Custom Query", "📋 Query History"]
        )
        
        if query_mode == "🎯 Query mẫu":
            sample_queries = get_sample_queries()
            selected_sample = st.selectbox(
                "Chọn query mẫu:",
                list(sample_queries.keys())
            )
            
            query_info = sample_queries[selected_sample]
            st.info(f"📝 {query_info['description']}")
            query = query_info['query']
            
            with st.expander("👀 Xem SQL Query"):
                st.code(query, language="sql")
                
        elif query_mode == "✏️ Custom Query":
            query = st.text_area(
                "Nhập SQL query:",
                height=200,
                placeholder="""SELECT 
    column1, 
    column2, 
    COUNT(*) as count
FROM `project.dataset.table`
WHERE condition = 'value'
GROUP BY column1, column2
ORDER BY count DESC
LIMIT 100""",
                help="Chỉ hỗ trợ SELECT queries. Không được sử dụng DELETE, DROP, etc."
            )
            
            # Validate query
            if query.strip():
                is_valid, message = validate_query(query)
                if is_valid:
                    st.success(f"✅ {message}")
                else:
                    st.error(f"❌ {message}")
                    
        else:  # Query History
            if 'query_history' not in st.session_state:
                st.session_state.query_history = []
            
            if st.session_state.query_history:
                selected_history = st.selectbox(
                    "Chọn query từ lịch sử:",
                    range(len(st.session_state.query_history)),
                    format_func=lambda x: f"Query {x+1}: {st.session_state.query_history[x][:50]}..."
                )
                query = st.session_state.query_history[selected_history]
                st.code(query, language="sql")
            else:
                st.info("📝 Chưa có query nào trong lịch sử")
                query = ""
        
        # Cấu hình hiển thị
        st.header("📊 Cấu hình hiển thị")
        max_rows = st.slider("Số dòng tối đa:", 100, 10000, 1000, 100)
        show_stats = st.checkbox("Hiển thị thống kê", True)
        auto_chart = st.checkbox("Tự động tạo biểu đồ", True)
    
    # Main content
    if st.button("🚀 Thực thi Query", type="primary", use_container_width=True):
        if not query.strip():
            st.warning("⚠️ Vui lòng nhập hoặc chọn một query")
            return
            
        # Validate query
        is_valid, message = validate_query(query)
        if not is_valid:
            st.error(f"❌ {message}")
            return
        
        # Lưu vào history
        if 'query_history' not in st.session_state:
            st.session_state.query_history = []
        if query not in st.session_state.query_history:
            st.session_state.query_history.insert(0, query)
            st.session_state.query_history = st.session_state.query_history[:10]  # Giữ 10 query gần nhất
        
        # Thực thi query
        with st.spinner("🔄 Đang truy vấn dữ liệu..."):
            df = run_bigquery_query(query, max_rows)
            
            if df is not None and not df.empty:
                # Metrics overview
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("📊 Số dòng", f"{len(df):,}")
                with col2:
                    st.metric("📋 Số cột", len(df.columns))
                with col3:
                    memory_mb = df.memory_usage(deep=True).sum() / 1024**2
                    st.metric("💾 Kích thước", f"{memory_mb:.2f} MB")
                with col4:
                    st.metric("⏱️ Thời gian", datetime.now().strftime("%H:%M:%S"))
                
                # Data preview
                st.subheader("📋 Kết quả truy vấn")
                
                # Column info
                with st.expander("ℹ️ Thông tin cột"):
                    col_info = pd.DataFrame({
                        'Cột': df.columns,
                        'Kiểu dữ liệu': df.dtypes,
                        'Null values': df.isnull().sum(),
                        'Unique values': df.nunique()
                    })
                    st.dataframe(col_info, use_container_width=True)
                
                # Main data table
                st.dataframe(df, use_container_width=True, height=400)
                
                # Download options
                col1, col2 = st.columns(2)
                with col1:
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Tải xuống CSV",
                        data=csv,
                        file_name=f"bigquery_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                with col2:
                    json_data = df.to_json(orient='records', indent=2)
                    st.download_button(
                        label="📥 Tải xuống JSON",
                        data=json_data,
                        file_name=f"bigquery_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )
                
                # Statistics
                if show_stats and len(df) > 0:
                    st.subheader("📈 Thống kê mô tả")
                    
                    # Numeric columns stats
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        st.write("**Cột số:**")
                        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
                    
                    # Categorical columns stats
                    cat_cols = df.select_dtypes(include=['object', 'string']).columns
                    if len(cat_cols) > 0:
                        st.write("**Cột phân loại:**")
                        for col in cat_cols[:3]:  # Chỉ hiển thị 3 cột đầu
                            value_counts = df[col].value_counts().head(10)
                            st.write(f"*{col}:*")
                            st.bar_chart(value_counts)
                
                # Auto visualization
                if auto_chart and len(df) > 1:
                    st.subheader("📊 Biểu đồ tự động")
                    
                    # Tìm cột phù hợp cho visualization
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    cat_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
                    
                    if len(numeric_cols) >= 1 and len(cat_cols) >= 1:
                        # Bar chart
                        if len(df) <= 50:  # Chỉ tạo bar chart nếu không quá nhiều dòng
                            fig_bar = create_bar_chart(df, cat_cols[0], numeric_cols[0], 
                                                     f"{numeric_cols[0]} theo {cat_cols[0]}")
                            st.plotly_chart(fig_bar, use_container_width=True)
                    
                    if len(numeric_cols) >= 2:
                        # Scatter plot
                        color_col = cat_cols[0] if cat_cols else None
                        fig_scatter = create_scatter_plot(df, numeric_cols[0], numeric_cols[1],
                                                        f"{numeric_cols[1]} vs {numeric_cols[0]}", color_col)
                        st.plotly_chart(fig_scatter, use_container_width=True)
                    
                    # Time series nếu có cột date/datetime
                    date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
                    if date_cols and numeric_cols:
                        df_sorted = df.sort_values(date_cols[0])
                        fig_line = create_line_chart(df_sorted, date_cols[0], numeric_cols[0],
                                                   f"{numeric_cols[0]} theo thời gian")
                        st.plotly_chart(fig_line, use_container_width=True)
                        
            elif df is not None:
                st.warning("⚠️ Query không trả về dữ liệu nào.")
            else:
                st.error("❌ Có lỗi xảy ra khi thực thi query.")

if __name__ == "__main__":
    main()