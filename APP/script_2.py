#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressionModel

spark = SparkSession.builder.appName("RandomForestRegressor").config("spark.driver.memory", "6g").getOrCreate()

df = spark.read.parquet("output.parquet")
df = df.na.drop()

# Define the input and output columns
features = ['vol_moving_avg', 'adj_close_rolling_med']
label = 'Volume'

# Convert the label column to numerical values
labelIndexer = StringIndexer(inputCol=label, outputCol="label").fit(df)
df = labelIndexer.transform(df)

# Create a vector assembler to combine the input features into a single vector column
assembler = VectorAssembler(inputCols=features, outputCol="features")
df = assembler.transform(df)

# Split the data into training and test sets
(trainingData, testData) = df.randomSplit([0.8, 0.2])
rf = RandomForestRegressor(numTrees=100)

model = rf.fit(trainingData)
model.write().overwrite().save("trained_model")
# model.save("trained_model")
model = RandomForestRegressionModel.load("trained_model")

# Test data prediction
predictions = model.transform(testData)

# Calculating MAE and MSE
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions)

evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mse")
mse = evaluator.evaluate(predictions)

print("Model Trained Successfully !!!")
print(f"MAE: {mae}")
print(f"MSE: {mse}")

spark.stop()

print("Script 2 is completed - Machine Learning Model has been made.")