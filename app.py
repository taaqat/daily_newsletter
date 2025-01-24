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
    today = datetime.datetime.now() - datetime.timedelta(days = 1)
    yesterday = today - datetime.timedelta(days = 1)
    previous_day = yesterday - datetime.timedelta(days = 1)
    previous_day, yesterday, today = previous_day.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    

    # * get news data from database
    raw_news = DataManager.fetch(yesterday, today) 

    if len(raw_news) < 20:
        raise ValueError("Returned news is not enough!")     # todo: send notification mail to admin
    
    # * summarize and generate newsletter by LLM
    in_message = DataManager.return_daily_raw_str(yesterday, raw_news)
    sys_prompt = prompt(DataManager.get_from_gsheet(previous_day).tolist()[0])

    # * test the api key balance (claude > gpt. we prefer claude)
    try:
        response = LlmManager.llm_api_call(LlmManager.create_prompt_chain(sys_prompt, LlmManager.claude_model), in_message)
    except:
        response = LlmManager.llm_api_call(LlmManager.create_prompt_chain(sys_prompt, LlmManager.gpt_model), in_message)
        
    
    pattern = r"<!DOCTYPE html>.*?</html>"
    match = re.search(pattern, response, re.DOTALL)
    print(response)
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
        try:
            DataManager.send_email(receiver_email = row['useremail'],
                                content = html_content,
                                status = 'succeeded',
                                date = yesterday)
        except:
            pass
        

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
        status = "failed",
        date = (datetime.datetime.now() - datetime.timedelta(days = 2)).strftime("%Y-%m-%d")
    )