def call_api(url, auth_key, payload = {}, headers=None, method="GET", cookies=None):
    import requests

    if headers is None:
        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_key, 
        }

    if method == "POST":
        response = requests.post(url, headers=headers, json=payload, cookies=cookies)
    elif method == "GET":   
        response = requests.get(url, headers=headers, json=payload, cookies=cookies)
    elif method == "PUT":   
        response = requests.put(url, headers=headers, json=payload, cookies=cookies)

    response.raise_for_status()
    return response