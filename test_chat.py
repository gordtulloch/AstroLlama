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
            try:
                data = json.loads(decoded_line[6:])
                msg_type = data.get("type")
                if msg_type in ["tool_start", "tool_result", "done"]:
                    print(decoded_line)
                elif msg_type == "token":
                    content = data.get("content", "").lower()
                    if "latitude" in content or "longitude" in content or "49." in content or "97." in content:
                        print(decoded_line)
            except:
                pass
