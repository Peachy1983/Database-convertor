#!/usr/bin/env python3
"""
Test script to verify Streamlit LinkColumn behavior with planning portal URLs
"""

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Test Planning Links", layout="wide")

st.title("🔗 Testing Planning Portal Links")

st.markdown("""
This page tests if Streamlit LinkColumn is working correctly with planning portal URLs.
Click the links below to verify they open properly in new tabs.
""")

# Test data with real planning portal URLs
test_data = [
    {
        'Authority': 'Barnet',
        'Reference': '19/2959/NMA',
        'URL': 'https://publicaccess.barnet.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=19/2959/NMA',
        'Description': 'Real Barnet planning application'
    },
    {
        'Authority': 'Westminster', 
        'Reference': '20/00100/FULL',
        'URL': 'https://idoxpa.westminster.gov.uk/online-applications/search.do?action=search&searchType=Application&reference=20/00100/FULL',
        'Description': 'Sample Westminster application'
    },
    {
        'Authority': 'Test Example',
        'Reference': 'TEST/001',
        'URL': 'https://www.google.com/search?q=planning+application+test',
        'Description': 'Test link that should definitely work'
    }
]

df = pd.DataFrame(test_data)

st.subheader("📊 Test Results Table")

# Configure LinkColumn exactly as in the main app
df_config = {
    'URL': st.column_config.LinkColumn(
        "Planning Portal",
        help="Click to view the full planning application details",
        display_text="View Application"
    )
}

# Display dataframe with LinkColumn
st.dataframe(df, column_config=df_config)

st.subheader("🔍 Manual Link Testing")
st.markdown("Test these links manually by clicking them:")

for i, row in df.iterrows():
    st.markdown(f"**{row['Authority']} - {row['Reference']}:**")
    st.markdown(f"[{row['Description']}]({row['URL']})")
    st.write(f"URL: `{row['URL']}`")
    st.write("---")

st.subheader("📋 Debugging Information")

st.code("""
LinkColumn Configuration:
{
    'URL': st.column_config.LinkColumn(
        "Planning Portal",
        help="Click to view the full planning application details", 
        display_text="View Application"
    )
}
""")

st.info("💡 **Test Instructions:**")
st.write("1. Click the 'View Application' links in the table above")
st.write("2. Check if they open in the same tab or new tab")  
st.write("3. Verify if the planning portal pages load correctly")
st.write("4. Test the manual links in the section below")

st.warning("⚠️ **Common Issues:**")
st.write("• Planning portal sites may be slow or temporarily unavailable")
st.write("• Some sites may block external referrers from Streamlit")
st.write("• LinkColumn may not open in new tabs by default")
st.write("• Some planning portals require specific user agents or cookies")