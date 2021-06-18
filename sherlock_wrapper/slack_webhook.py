"""Send messages to a Slack webhook."""

import sys, requests, json
import argparse

def send(url, message):
    """Send a single message to the given webook."""
    data = {'channel': '#monitoring-lasair-ztf', 'text': message}

    response = requests.post(url, data=json.dumps(data),
        headers={'Content-Type': 'application/json'})

    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )

if __name__ == "__main__":
    """Read from stdin and send each line as a message."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-u', '--url', required=True, type=str, help='Webhook URL')
    conf = vars(parser.parse_args())

    for line in sys.stdin:
        send(conf['url'], line.rstrip())
        
