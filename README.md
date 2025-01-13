## Daily News Digest Sender

In order to exploit the daily-scraped news and article data more efficiently, we construct a daily nes digest sender which summarizes the news daily and send the digest to all subscribers at 9:30 am GMT +8.

/taaqat/daily_newsletter
├── README.md
├── app.py
├── manager.py
├── requirements.txt
└── test.py

### `app.py`
This file is the main python script where the codes of generating summary, updating to google sheet database, and sending emails are intergrated.

Basic logic:

- if the number of news exceeds 20, we generate the news digest by Claude and then send the result to all subscribers.

- else, raise error that informs administrator that the returned news is not adequate enough.

- try/except condition: if there is any error, only notify the administrator.

### `manager.py`
This file defines a series of utilities, including:

- `DataManager` defines functions that

    - communicate with III database
    - transform database to string
    - get users (subscribers) information (email)
    - send emails 
    - update the digest to google sheet

- `LlmManager` defines functions that

    - generates prompt chain that is required for LangChain
    - call Claude api and return JSON format output

- `prompt` defines the prompt that instructs Claude how to generate the digest.