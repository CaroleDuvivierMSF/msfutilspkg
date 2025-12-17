# build_udf_payload.py

import json

SOURCE_FILE = "src/udf_source.py"
OUTPUT_FILE = "udf_payload.json"
MAIN_FILE_NAME = "udf_source.py"

print(f"Reading source code from {SOURCE_FILE}...")
try:
    with open(SOURCE_FILE, 'r') as f:
        udf_code_content = f.read()
except FileNotFoundError:
    print(f"Error: Source file not found at {SOURCE_FILE}. Exiting.")
    exit(1)

# Construct the required JSON payload structure
payload_data = {
  "properties": {
    "language": "Python",
    "mainFile": MAIN_FILE_NAME,
    "files": [
      {
        # The actual code content is embedded here as a string
        "content": udf_code_content,
        "path": MAIN_FILE_NAME,
        "mimeType": "text/x-python"
      }
    ]
  },
  "type": "UserDataFunction"
}

print(f"Saving deployment payload to {OUTPUT_FILE}...")
with open(OUTPUT_FILE, 'w') as out_f:
    json.dump(payload_data, out_f, indent=2)

print("✅ UDF payload built successfully.")