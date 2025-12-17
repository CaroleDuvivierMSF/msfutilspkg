# deploy_udf.py

import os
import requests
import json
import base64

# --- Configuration ---
PAYLOAD_FILE = "udf_payload.json"
API_BASE_URL = "https://api.fabric.microsoft.com/v1/workspaces"

# --- Environment Variables (Must be set in your shell) ---
try:
    FABRIC_WORKSPACE_ID = "64a3cd7e-33e8-4471-b880-c4f3ef225db1"
    FABRIC_UDF_NAME = "newfunctiontestcarole"
    FABRIC_UDF_ID = "ba3b9c64-3212-4cd8-a8f9-3b8dc24a241a"
except KeyError as e:
    print(f"Error: Missing required environment variable: {e}")
    exit(1)


def find_or_create_udf_item(token):
    """Checks for existing UDF and creates it if it doesn't exist."""
    headers = {"Authorization": f"Bearer {token}"}
    
    # 1. Search for existing UDF item
    search_url = (
        f"{API_BASE_URL}/{FABRIC_WORKSPACE_ID}/items?"
        f"$filter=name eq '{FABRIC_UDF_NAME}'&$select=id"
    )
    print(f"🔎 Checking for existing UDF item named: {FABRIC_UDF_NAME}")
    response = requests.get(search_url, headers=headers)
    response.raise_for_status()
    
    items = response.json().get('value', [])
    if items:
        udf_id = items[0]['id']
        print(f"👍 UDF item found with ID: {udf_id}")
        return udf_id

    # 2. Create new UDF item
    print(f"🆕 UDF item not found. Creating new item: {FABRIC_UDF_NAME}")
    create_url = f"{API_BASE_URL}/{FABRIC_WORKSPACE_ID}/items"
    create_body = {
        "displayName": FABRIC_UDF_NAME,
        "type": "UserDataFunction"
    }
    
    response = requests.post(create_url, headers=headers, json=create_body)
    response.raise_for_status()
    
    udf_id = response.json().get('id')
    if not udf_id:
        print(f"Error creating UDF item: {response.text}")
        exit(1)
        
    print(f"✅ UDF item created with ID: {udf_id}")
    return udf_id

def update_udf_definition(token, udf_id):
    """Updates the definition (code) of the existing UDF item."""
    print("📝 Preparing UDF code definition...")
    
    # 1. Read and Base64 Encode the payload file
    try:
        with open(PAYLOAD_FILE, 'rb') as f:
            payload_content = f.read()
        udf_payload_b64 = base64.b64encode(payload_content).decode('utf-8')
    except FileNotFoundError:
        print(f"Error: Payload file '{PAYLOAD_FILE}' not found.")
        exit(1)

    # 2. Construct the API Body with the Base64 payload
    api_body = {
        "definition": {
            "parts": [
                {
                    "path": "udf_source.py", # This must match the name in build_udf_payload.py
                    "payload": udf_payload_b64,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    
    # 3. Perform the POST update
    update_url = f"{API_BASE_URL}/{FABRIC_WORKSPACE_ID}/userDataFunctions/{udf_id}/updateDefinition"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    print(f"🚀 Updating UDF Definition in Fabric (ID: {udf_id})...")
    response = requests.post(update_url, headers=headers, json=api_body)
    
    if response.status_code == 202:
        print("✅ UDF definition update request ACCEPTED by Fabric (202).")
        print("Please wait a moment and refresh the Fabric UI to see changes.")
        # If the update still fails, the error is being logged inside Fabric.
    elif not response.ok:
        # Existing error handling for 400, 401, 403, etc.
        print(f"Error updating UDF definition. Status: {response.status_code}")
        print(f"Response: {response.text}")
        response.raise_for_status() 
    else:
        print("✅ UDF definition updated successfully.")
    
    print("✅ UDF definition updated successfully.")


def main():
    try:
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6InJ0c0ZULWItN0x1WTdEVlllU05LY0lKN1ZuYyIsImtpZCI6InJ0c0ZULWItN0x1WTdEVlllU05LY0lKN1ZuYyJ9.eyJhdWQiOiJodHRwczovL2FwaS5mYWJyaWMubWljcm9zb2Z0LmNvbSIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzRkOWRkMWFmLTgzY2UtNGU5Yi1iMDkwLWIwNTQzY2NjMmIzMS8iLCJpYXQiOjE3NjMzODM4MDMsIm5iZiI6MTc2MzM4MzgwMywiZXhwIjoxNzYzMzg4ODg4LCJhY2N0IjowLCJhY3IiOiIxIiwiYWlvIjoiQVhRQWkvOGFBQUFBUVlVQ0pxYllmYW1LYzZoR2xteDhYb3VZcXRCeEdjVGE5WVpsN3R4UEpRN0xsRFVCZ2pzY2tqbk9SbnhJdk9tQk1IL2Z2OElVVmtJWGFDa3R3eG94Skx4ZDdlb1pCayt5d2kyeUwyZWszc1lIOUlOVVRSSkJCcTVkcGpXVkQzMDZwUFVWMlBERFV6OU5GUzNxVmJoTGlRPT0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiMThmYmNhMTYtMjIyNC00NWY2LTg1YjAtZjdiZjJiMzliM2YzIiwiYXBwaWRhY3IiOiIwIiwiZGV2aWNlaWQiOiI3MzQwZjcwMi05MDMzLTQ5ZjYtYWE2MS00NTkxM2M5MmNmZjUiLCJmYW1pbHlfbmFtZSI6IkR1dml2aWVyIiwiZ2l2ZW5fbmFtZSI6IkNhcm9sZSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjgxLjI0Ni43MC4yMzQiLCJuYW1lIjoiQ2Fyb2xlIER1dml2aWVyIChGYWJyaWMgQWRtaW4pIiwib2lkIjoiOWIwODk2ZTAtMWYwOC00NGMzLTgxNjMtYTBhNWEwZDJlNTk4Iiwib25wcmVtX3NpZCI6IlMtMS01LTIxLTc1MDU4NDQ1MC0xNDYxOTg2MTUxLTE0MzUzMjUyMTktNDE4MTQiLCJwdWlkIjoiMTAwMzIwMDUxNDU5MEMwQSIsInJoIjoiMS5BUkFBcjlHZFRjNkRtMDZ3a0xCVVBNd3JNUWtBQUFBQUFBQUF3QUFBQUFBQUFBQVFBS1lRQUEuIiwic2NwIjoiQXBwLlJlYWQuQWxsIENhcGFjaXR5LlJlYWQuQWxsIENhcGFjaXR5LlJlYWRXcml0ZS5BbGwgQ29ubmVjdGlvbi5SZWFkLkFsbCBDb25uZWN0aW9uLlJlYWRXcml0ZS5BbGwgQ29udGVudC5DcmVhdGUgRGFzaGJvYXJkLlJlYWQuQWxsIERhc2hib2FyZC5SZWFkV3JpdGUuQWxsIERhdGFmbG93LlJlYWQuQWxsIERhdGFmbG93LlJlYWRXcml0ZS5BbGwgRGF0YXNldC5SZWFkLkFsbCBEYXRhc2V0LlJlYWRXcml0ZS5BbGwgR2F0ZXdheS5SZWFkLkFsbCBHYXRld2F5LlJlYWRXcml0ZS5BbGwgSXRlbS5FeGVjdXRlLkFsbCBJdGVtLkV4dGVybmFsRGF0YVNoYXJlLkFsbCBJdGVtLlJlYWRXcml0ZS5BbGwgSXRlbS5SZXNoYXJlLkFsbCBPbmVMYWtlLlJlYWQuQWxsIE9uZUxha2UuUmVhZFdyaXRlLkFsbCBQaXBlbGluZS5EZXBsb3kgUGlwZWxpbmUuUmVhZC5BbGwgUGlwZWxpbmUuUmVhZFdyaXRlLkFsbCBSZXBvcnQuUmVhZFdyaXRlLkFsbCBSZXBydC5SZWFkLkFsbCBTdG9yYWdlQWNjb3VudC5SZWFkLkFsbCBTdG9yYWdlQWNjb3VudC5SZWFkV3JpdGUuQWxsIFRhZy5SZWFkLkFsbCBUZW5hbnQuUmVhZC5BbGwgVGVuYW50LlJlYWRXcml0ZS5BbGwgVXNlclN0YXRlLlJlYWRXcml0ZS5BbGwgV29ya3NwYWNlLkdpdENvbW1pdC5BbGwgV29ya3NwYWNlLkdpdFVwZGF0ZS5BbGwgV29ya3NwYWNlLlJlYWQuQWxsIFdvcmtzcGFjZS5SZWFkV3JpdGUuQWxsIiwic2lkIjoiMDA5OGM1NzktZjVjNS03Y2JhLWFmNGYtMTljYTZmMWEyMGYxIiwic2lnbmluX3N0YXRlIjpbImR2Y19tbmdkIiwiZHZjX2NtcCIsImttc2kiXSwic3ViIjoiNmlaQ3pDMjI3MmoxWTNHT2x6ZUFSUVg5aEF0VDlmbVktdWtkTGpGWGJkWSIsInRpZCI6IjRkOWRkMWFmLTgzY2UtNGU5Yi1iMDkwLWIwNTQzY2NjMmIzMSIsInVuaXF1ZV9uYW1lIjoiY2R1dml2aWVyZGF0YUBicnVzc2Vscy5tc2Yub3JnIiwidXBuIjoiY2R1dml2aWVyZGF0YUBicnVzc2Vscy5tc2Yub3JnIiwidXRpIjoiV3B6OHNuLTY1a0tmZ19yMWtyY3lBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19hY3RfZmN0IjoiMyA1IiwieG1zX2Z0ZCI6ImZaaDRaQjZoTWhFcG9nM0lPc2pBLW9HOHplZHdhY2V3NzJKbU1UaVRFZ2NCYzNkbFpHVnVZeTFrYzIxeiIsInhtc19pZHJlbCI6IjEgMiIsInhtc19zdWJfZmN0IjoiNCAzIn0.LMb_7OUWkSK02YX30Y8Np2aHone4sUYqtRpwq6ZiT__VDaNgk90C1v-8gJG8C7jdZIDGxIxuJedw8i4UDeb1dGHuKk0THFKsbiZMoxnHFnpN5Xicn0cUvpabWXeOrYIIjnUPN4ef-SQJ8RzYHBI3VCiOHt_bWm1Kj4P7bL0-j0idsoBE3CeXhr7UM2tE9NeVVWILn5a105wkaNHqvy1WSgIMlm-_evx-KHTIeJMYYyIlkyCdxQahbMknlpJvtvAVqj0fl6x-cGAhjYRtn4b7pVd-FNS1BxCrdzH0eYpO5Vv9Ek0RKcGyxDduEgn6-fL_B7_UhjvLNUtidsmIjdLxSg"
        udf_id = "ba3b9c64-3212-4cd8-a8f9-3b8dc24a241a"
        update_udf_definition(token, udf_id)
        
    except requests.exceptions.HTTPError as e:
        print(f"\n--- Deployment Failed ---")
        print(f"An HTTP error occurred: {e}")
        print("Check your secrets and Service Principal permissions.")
        exit(1)
    except Exception as e:
        print(f"\n--- Deployment Failed ---")
        print(f"An unexpected error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    main()