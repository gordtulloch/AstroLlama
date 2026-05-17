import requests
import json

url = "http://localhost:8080/api/chat"
payload = {
    "message": "Whats the latitude and longitude of Winnipeg Manitoba Canada",
    "settings": {"hide_tool_bubbles": False}
}

response = requests.post(url, json=payload, stream=True)
for line in response.iter_lines():
    if line:
        decoded_line = line.decode("utf-8")
        if decoded_line.startswith("data: "):
            print(decoded_line)
