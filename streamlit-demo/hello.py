import streamlit as st

hello = """# hello
## streamlit
---
"""
st.write(hello)
code = """
print('hello')
"""
st.code(code, language='python')
st.code('select * from t1', language='sql')


st.text_input("your SQL:", '')

st.text_area("your SQL:", '')

if st.button('click'):
    st.write('clicked')