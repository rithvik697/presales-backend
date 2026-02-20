import requests

url = "http://localhost:5000/api/login"
payload = {
    "username": "aryan",
    "email": "aryanfall2022@gmail.com",
    "password": "aryan123"
}

try:
    response = requests.post(url, json=payload)
    print(f"Status Code: {response.status_code}")
    print(f"Response Body: {response.text}")
except Exception as e:
    print(f"Error: {e}")
