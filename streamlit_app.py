import streamlit as st
import pandas as pd
import google.generativeai as genai
from pypdf import PdfReader

# 1. Page Settings
st.set_page_config(page_title="AI Market Intelligence Pro", layout="wide")

# 2. Connection Setup
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("🔑 Configuration Error: GOOGLE_API_KEY not found in Streamlit Secrets.")
    st.stop()

genai.configure(api_key=st.secrets["GOOGLE_API_KEY"])

@st.cache_resource
def load_best_model():
    """Automatically finds the best available model to avoid 404 errors."""
    try:
        # Ask Google which models are available for this API Key
        available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        
        # Preference order
        preferences = ['models/gemini-1.5-flash', 'models/gemini-1.5-pro', 'models/gemini-pro']
        
        for pref in preferences:
            if pref in available_models:
                return genai.GenerativeModel(pref), pref
        
        # Fallback to the first available if none of the above are found
        if available_models:
            return genai.GenerativeModel(available_models[0]), available_models[0]
    except Exception as e:
        st.error(f"Failed to fetch models: {e}")
    return None, None

model, model_name = load_best_model()

# 3. Fluent English UI
st.title("🤖 AI Real Estate Market Analyst")
st.subheader("Professional Market Insights & Data Intelligence")
if model_name:
    st.caption(f"Connected to AI Engine: `{model_name}`")
st.markdown("---")

# 4. Data Upload
uploaded_file = st.file_uploader("Upload Property Data (CSV, Excel, or PDF)", type=['csv', 'xlsx', 'pdf'])

if uploaded_file:
    data_payload = ""
    file_ext = uploaded_file.name.split('.')[-1].lower()

    try:
        if file_ext == 'pdf':
            reader = PdfReader(uploaded_file)
            # Take text from first 5 pages
            data_payload = " ".join([p.extract_text() for p in reader.pages[:5]])
            st.success("✅ PDF Analysis Ready.")
        else:
            df = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
            st.success("✅ Spreadsheet Loaded.")
            st.subheader("Data Preview")
            st.dataframe(df.head(5))
            # Convert sample data to text
            data_payload = df.head(20).to_string()

        # 5. Analysis Execution
        st.markdown("---")
        if st.button("🚀 Generate Strategic Market Report"):
            if not model:
                st.error("AI Model not initialized. Please check your API key.")
            else:
                with st.spinner('AI is analyzing market trends...'):
                    try:
                        prompt = f"""
                        Role: Professional Florida Real Estate Consultant.
                        Source: {uploaded_file.name}
                        
                        Context: Analyze the property data provided below and write a professional report in English.
                        
                        Data:
                        {data_payload}
                        
                        Report Requirements:
                        1. MARKET SNAPSHOT: Summarize the dataset (e.g., North Port area inventory).
                        2. PRICING INTELLIGENCE: Analyze price points, averages, and outliers.
                        3. GEOGRAPHIC INSIGHTS: Highlight specific subdivisions or zip codes showing activity.
                        4. INVESTMENT STRATEGY: Provide 3 professional recommendations for a buyer/investor.
                        
                        Format the output with bold headers and clean bullet points.
                        """
                        
                        response = model.generate_content(prompt)
                        st.markdown("### 📊 AI Strategic Intelligence Report")
                        st.write(response.text)
                        st.balloons()
                        
                    except Exception as e:
                        st.error(f"Analysis Error: {e}")
                        st.info("Check if your API Key has 'Gemini' enabled in the Google AI Studio.")

    except Exception as e:
        st.error(f"File Processing Error: {e}")

else:
    st.info("💡 Pro Tip: Upload your MLS Export or Market Report to begin.")
