In this Repository, 3 things are shown: 
1. Download dataset from Kaggle & convert all the CSV files into a single parquet file using Spark
2. Build machine learning models based on the parquet files & save the model
3. And created an API endpoint that uses the model to produce real time results.

API Endpoint template:
POST: http://0.0.0.0:8000/
Json format 
{
    "vol_moving_avg": "48945",
    "adj_close_rolling_med": 35
}

By default Running the docker container exposes the API endpoint based on the previous trained model.
1. To download & convert csv files to parquet   - python script_1.py
2. To build a machine learning model            - python script_2.py
3. Sample Automation                            - python sample_automation.py
4. Full Automation                              - python full_automation.py
5. To activate the API server                   - python FastAPI_Server.py


Docker Commands:
1. docker-compose build
2. docker-compose up -d

Activate Virtual Environment: source venv/bin/activate

Architecture of this whole data pipeline
![alt text](https://github.com/sunilprakash97/Data-Pipeline/blob/main/Flowchart.png)