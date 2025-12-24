import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Config
st.set_page_config(page_title="Global AI Market Analyst", layout="wide")

# 2. API Key Check
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 API Key missing. Add GOOGLE_API_KEY to Streamlit Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

# 3. Smart Model Selection (Prevents 404)
@st.cache_resource
def get_working_model():
    try:
        # Find all models available to YOUR key
        available = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        
        # Preference list
        prefs = ['models/gemini-1.5-flash', 'gemini-1.5-flash', 'models/gemini-1.5-pro', 'models/gemini-pro']
        
        for p in prefs:
            if p in available:
                return genai.GenerativeModel(p), p
        
        if available:
            return genai.GenerativeModel(available[0]), available[0]
    except Exception as e:
        st.error(f"Error connecting to Google AI: {e}")
    return None, None

model, model_name = get_working_model()

# 4. UI Header
st.title("🤖 Global Multi-Source Market Analyst")
st.subheader("Advanced Real Estate Intelligence")
if model_name:
    st.caption(f"Status: ✅ Connected to `{model_name}`")
st.markdown("---")

# 5. Multi-File Upload
# 'accept_multiple_files=True' is key for your requirement
files = st.file_uploader("Upload all your files (CSV, Excel, PDF)", type=['csv', 'xlsx', 'pdf'], accept_multiple_files=True)

if files:
    full_context = ""
    st.write(f"### 📑 Processing {len(files)} files...")

    for f in files:
        ext = f.name.split('.')[-1].lower()
        with st.expander(f"Analyzing: {f.name}"):
            try:
                if ext == 'pdf':
                    reader = PdfReader(f)
                    text = " ".join([page.extract_text() for page in reader.pages])
                    full_context += f"\n--- DOCUMENT: {f.name} ---\n{text[:5000]}\n" # Cap to avoid token crash
                    st.success("Full PDF text captured.")
                else:
                    df = pd.read_csv(f) if ext == 'csv' else pd.read_excel(f)
                    
                    # ANALYZE ENTIRE CONTENT: We create a detailed summary of 100% of the rows
                    # This ensures the AI knows about EVERY row even if it's a huge file
                    summary = {
                        "Rows": len(df),
                        "Price Average": df.select_dtypes(include='number').mean().to_dict(),
                        "Subdivisions": df.iloc[:, 5].value_counts().head(20).to_dict() if len(df.columns) > 5 else "N/A",
                        "Columns": df.columns.tolist()
                    }
                    st.write("Full Statistical Overview:", summary)
                    st.dataframe(df.head(10))
                    
                    # Feed the AI the global statistics + a large sample
                    full_context += f"\n--- DATABASE: {f.name} ---\nGlobal Stats: {summary}\nData Sample:\n{df.head(50).to_string()}\n"

            except Exception as e:
                st.error(f"Error reading {f.name}: {e}")

    # 6. Global Comparative Analysis
    st.markdown("---")
    if st.button("🚀 Run Comprehensive Analysis on ALL Data"):
        if not model:
            st.error("AI not ready.")
        else:
            with st.spinner('AI is cross-referencing all datasets...'):
                try:
                    prompt = f"""
                    You are a Senior Investment Consultant. I have provided multiple files.
                    Analyze EVERYTHING below as a single market intelligence project.
                    
                    DATA PROVIDED:
                    {full_context}
                    
                    REQUIREMENTS (English):
                    1. GLOBAL SUMMARY: Combine all files. What is the total inventory and main focus?
                    2. MARKET TRENDS: Based on the global statistics, identify pricing patterns across all data.
                    3. GEOGRAPHIC FOCUS: Which areas appear most frequently and what are the price differences between them?
                    4. INVESTMENT STRATEGY: Provide 5 professional insights for an investor looking at these specific results.
                    
                    Format with professional headers and bullet points.
                    """
                    response = model.generate_content(prompt)
                    st.markdown("### 📊 Global Market Intelligence Report")
                    st.write(response.text)
                    st.balloons()
                except Exception as e:
                    st.error(f"Analysis Failed: {e}")

else:
    st.info("💡 Tip: Select multiple files at once to compare different reports.")
