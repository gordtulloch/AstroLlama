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
                # Filter for requested types
                if msg_type in ["tool_start", "tool_result", "done"]:
                    print(decoded_line)
                elif msg_type == "token":
                    text = data.get("text", "")
                    # Print tokens related to coordinates
                    if any(c in text for c in ["49", "97", "Latitude", "Longitude"]):
                        print(decoded_line)
            except:
                pass
