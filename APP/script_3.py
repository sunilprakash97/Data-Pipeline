from pyspark.sql import SparkSession
from fastapi import FastAPI, status
from pydantic import BaseModel, root_validator
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.linalg import Vectors

app = FastAPI()

spark = SparkSession.builder.appName("myApp").getOrCreate()
model = RandomForestRegressionModel.load("trained_model")

class PredictionRequest(BaseModel):
    vol_moving_avg: float
    adj_close_rolling_med: float

class PredictionResponse(BaseModel):
    prediction: float
    @root_validator
    def validate_prediction(cls, values):
        values['prediction'] = round(values['prediction'], 2)
        return values


@app.post("/", response_model=PredictionResponse, status_code=status.HTTP_200_OK)
def predict(request: PredictionRequest):
    
    # Converting request payload to a PySpark vector
    features = Vectors.dense([
        request.vol_moving_avg,
        request.adj_close_rolling_med
    ])
    
    prediction = model.predict(features)
    return PredictionResponse(prediction = prediction)

print("Successful !!!")
