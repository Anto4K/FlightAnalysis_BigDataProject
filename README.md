# ✈️ Flight Analysis - Big Data Project

In-depth analysis and interactive visualization of flights in the United
States using Big Data technologies and Machine Learning.

## 📚 Description

This project was developed as an activity for the course "Models and
Techniques for Big Data" and focuses on the analysis of U.S. flight data
from 2013, leveraging the capabilities of **Apache Spark** for
distributed processing and **Streamlit** for building interactive web
dashboards.\
The goal is to provide a user-friendly platform for exploring
statistics, delays, performance, and predictive models related to
flights.

## 📁 Project Structure

    .
    ├── spark/                      
    │   ├── ml/                     
    │   │   ├── classificatoreRandomForest.py
    │   │   ├── clustering.py
    │   │   └── metodiAux.py
    │   │
    │   ├── query/                  # Queries and analyses organized by topic
    │   │   ├── aeroporti_analysis/
    │   │   ├── dashboard_analysis/
    │   │   ├── route/
    │   │   ├── statistiche_annuali/
    │   │   └── allQuery.py
    │   │
    │   └── utils/                  # Common utilities and support files
    │       ├── citta_lat_long.csv
    │       ├── create_session.py
    │       ├── lista_nomi_aeroporti.txt
    │       ├── preprocessing.py
    │       └── utils.py
    │
    ├── pages/                      # Streamlit interface divided into sections
    │   ├── 1_📊_Analisi_mensile.py       # Monthly data analysis
    │   ├── 2_🔍_Ricerca_voli.py          # Flight search
    │   ├── 3_🌍_Analisi_aeroporti.py     # City and airport statistics
    │   ├── 4_📆_Analisi_annuale.py       # Aggregated annual analyses
    │   ├── 5_🤖_Classificazione_ML.py    # Prediction with ML models
    │   └── 6_🧩_Clustering_ML.py         # Segmentation with clustering
    │
    ├── data/                       # Input CSV datasets (not uploaded to GitHub)
    │
    ├── README.md
    └── Home.py                      # Main entry point of the Streamlit app

## ⚙️ Technologies and Libraries Used

-   **Python**: main programming language
-   **Apache Spark (PySpark)**: for distributed data processing
-   **Streamlit**: for building the web interface
-   **Plotly**: interactive charts (bar, pie, scatter, heatmaps)
-   **Pydeck**: for geographic maps integrated in Streamlit
-   **airportsdata**: to retrieve detailed airport data
-   **Spark MLlib**: for Machine Learning algorithms (Random Forest,
    K-Means)
-   **pandas, numpy**: for support operations and local data
    manipulation

## 🚀 Running the Project

1.  Clone the repository\
2.  Make sure you have Apache Spark and Python ≥ 3.8 installed\
3.  Install the required dependencies:

``` bash
pip install streamlit plotly pydeck pandas numpy airportsdata
```

4.  Launch the application:

``` bash
streamlit run Home.py
```

## 📊 Page Contents and Results

### 1. 📈 Monthly Flight Analysis

-   Metrics on flights, delays, distances, and duration
-   Delay classification in 15-minute categories
-   Flight status (on time, delayed, cancelled, diverted)
-   Main causes of delays (airline, weather, NAS, security, late
    aircraft)

### 2. 🔍 Flight Search

-   Custom search with interactive map and detailed information
-   Separate visualization for on-time, delayed, cancelled, or diverted
    flights

### 3. 🌍 Cities and Airports

-   Statistics by city: departures/arrivals, average delays, airports
-   Most frequent destinations and interactive geographic map

### 4. 📆 Annual Statistics

-   Monthly distribution of flight statuses
-   Weekly heatmap (days vs months)
-   Map of the busiest cities

### 5. 🤖 Delay Prediction (ML)

-   **Random Forest** model for binary classification (delay \>15 min)
-   Class balancing with under-sampling
-   Metrics: Accuracy, Precision, Recall, F1 Score, Confusion Matrix

### 6. 🧩 Flight Clustering

-   **K-Means** algorithm with silhouette score
-   Scatter plots and histograms for different *k* values

------------------------------------------------------------------------

## 👤 Author

🎯 *Project developed by Rocco Pio Vardaro and Antonio Pio Francica as
part of the course on Analysis and Techniques for Big Data*
