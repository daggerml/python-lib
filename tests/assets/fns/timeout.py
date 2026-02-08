import json

if __name__ == "__main__":
    print(json.dumps({"status": "running", "error": None}, separators=(",", ":")))
