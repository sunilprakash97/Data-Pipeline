import requests

url = "http://localhost:8000/"

payload = {
    "vol_moving_avg": 123.45,
    "adj_close_rolling_med": 678.90
}

response = requests.post(url, json=payload)

print(response.status_code)
print(response.json())