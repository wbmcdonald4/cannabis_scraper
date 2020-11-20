import os
from os import environ

import requests


class Slack:

    team_members = {
        'brendan': 'U01E6QGL7PX',
    }

    def __init__(self):
        self.slack_token = environ.get("SLACK_TOKEN")

    def send_message(self, message='test msg', channel='sfdc_test_channel'):
        data = {
            'token': self.slack_token,
            'channel': channel,
            'as_user': True,
            'text': message
        }
        requests.post(url='https://slack.com/api/chat.postMessage',
                      data=data)

    def stream_messages(self, message_list=[], channel='sfdc_test_channel'):
        for message in message_list:
            data = {
                'token': self.slack_token,
                'channel': channel,
                'as_user': True,
                'text': message
            }
            requests.post(url='https://slack.com/api/chat.postMessage',
                          data=data)

