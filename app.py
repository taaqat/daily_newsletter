import datetime 
import pandas as pd
from manager import (
    DataManager,
    LlmManager,
    prompt
)
import re

def main():

    # * get date data for yesterday and today
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = 1)
    yesterday, today = yesterday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

    # * get news data from database
    # raw_news = DataManager.fetch(yesterday, today)
    raw_news = DataManager.fetch(yesterday, today) # todo 記得改回 today, tomorrow

    if len(raw_news) < 20:
        raise ValueError("Returned news is not enough!")     # todo: send notification mail to admin
    
    # * summarize and generate newsletter by LLM
    in_message = DataManager.return_daily_raw_str(yesterday, raw_news)
    response = LlmManager.llm_api_call(LlmManager.create_prompt_chain(prompt), in_message)
    pattern = r"<!doctype html>.*?</html>"
    match = re.search(pattern, response, re.DOTALL)

    if match:
        html_content = match.group(0)
        print("Daily newsletter generated")
    else:
        raise ValueError("Failed to extract HTML body!")

    # * post the response to III Database
    DataManager.update_gsheet(
        yesterday, html_content
    )

    user_data = DataManager.get_user_data()
    for _, row in user_data.iterrows():
        DataManager.send_email(receiver_email = row['useremail'],
                               content = html_content,
                               status = 'succeeded')
        

# * 主程式：
try:
    main()
except Exception as e:
    DataManager.send_email(
        receiver_email = 'huang0jin@gmail.com',
        content = f"""
Some error happened:
<br>
{str(e)}
""",
        status = "failed"
    )