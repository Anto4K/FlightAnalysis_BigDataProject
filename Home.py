import os
import streamlit as st
from PIL import Image

st.set_page_config(
    page_title="Pagina Iniziale"
)

# Aggiungi icona aereo sopra la sidebar
st.sidebar.markdown(
    """
    <div style="text-align: center; font-size: 100px;">
        ✈️
    </div>
    """,
    unsafe_allow_html=True
)

# Titolo dell'app
st.markdown(
    """
    <div class="title" style="color: blue;">Benvenuto nell'app Analisi Voli Aerei</div>
    """,
    unsafe_allow_html=True,
)

file_path = os.path.join(os.path.dirname(__file__), 'data/logo_voli.jpg')
logo = Image.open(file_path)

# Stile personalizzato con CSS
st.markdown(
    """
    <style>
    .title {
        font-size: 40px;
        color: #333333;
        text-align: center;
        font-weight: bold;
        margin-bottom: 20px;
    }
    .description {
        font-size: 18px;
        color: #555555;
        text-align: center;
        margin-bottom: 30px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Layout della pagina principale
col1, col2, col3 = st.columns([1, 3, 1])

#with col2:
st.image(logo, use_container_width=True)

st.markdown(
    """
    <div class="description">
    Esplora i dati sui voli, analizza ritardi, scopri le performance aeree e ottieni insight utili per i tuoi viaggi o analisi professionali. 
    Questa applicazione offre strumenti intuitivi e potenti per comprendere il complesso mondo dell'aviazione.
    </div>
    """,
    unsafe_allow_html=True,
)

# Messaggio footer
st.markdown(
    "<hr style='border: 1px solid #e0e0e0;'>",
    unsafe_allow_html=True,
)
st.markdown(
    "<p style='text-align: center; font-size: 14px; color: #999;'>\u00a9 2025 - Analisi Voli Aerei. Tutti i diritti riservati.</p>",
    unsafe_allow_html=True,
)
