import requests, json

def send(url, message):
    data = {'channel': '#monitoring-lasair-ztf', 'text': message}

    response = requests.post(url, data=json.dumps(data),
        headers={'Content-Type': 'application/json'})

    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )

if __name__ == "__main__":
    send('Test, please ignore')
