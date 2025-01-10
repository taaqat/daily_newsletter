import requests
from dotenv import load_dotenv
import urllib3
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import pandas as pd
import datetime
import pymongo
import streamlit as st
from streamlit_gsheets import GSheetsConnection
from pymongo.mongo_client import MongoClient
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
import time
import os

load_dotenv()

class DataManager:
    @staticmethod
    def fetch(
            start_date, 
            end_date, 
            output_format = 'fulltext', 
            search_scope = 'title,content'
            ):

        url = 'http://61.64.60.30/news-crawler/api/news_summary/?'

        headers = {
            'Authorization': os.getenv("III_KEY")
        }

        result_dfs = []


        # *** Set up retries for the connection ***
        retry_strategy = Retry(
            total=3,  # Number of retries
            status_forcelist=[429, 500, 502, 503, 504]  # Retry on these status codes
        )

        adapter = HTTPAdapter(max_retries = retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)
            
        # *** Fetching data recursively until return ***
        print(f'Fetching daily news data from {start_date} to {end_date}...')

        page = 1
        while True:
            # Keywords 使用者用換行分隔代表 OR，不過 https POST 時要記得改成用 pipeline 符號

            end_point_params = {
                'start_date': start_date,
                'end_date': end_date,
                'search_scope': search_scope,
                'output_format': output_format,
                'page': page}

            
            try:
                response = http.post(url, end_point_params, headers = headers)
            except urllib3.exceptions.NewConnectionError as e:
                print(f"Connection failed: {e}")
                print(response.content)
            
            # PRINT HTTPS Error Messege if we encounter errors
            if response.status_code == 200:
                pass 
            else:
                print(response.content)

            # Clean data into pd.DataFrame format
            data = response.json()['data']

            df = pd.DataFrame(data)
            result_dfs.append(df)


            if data == []:
                break

            page += 1
        
        # Concatenate all dataframes from all pages 
        result_df = pd.concat(result_dfs)
        # result_df = result_df.drop_duplicates(subset = ['title'])

        # Transform the publish time column to datetime 
        result_df['published_at'] = pd.to_datetime(result_df['published_at'])

        print(f"All required data collected. {len(result_df)} rows in total.")

        return result_df
    
    @staticmethod
    def return_daily_raw_str(
        day: str, 
        data: pd.DataFrame
        ) -> str:

        contents = []
        if type(day) == str:
            date = datetime.datetime.strptime(day, "%Y-%m-%d").date()
        else:
            date = day



        for index, row in data.iterrows():
            # print(f"Row {index}: date={row['date']}, 重點摘要={row['重點摘要']}, 關鍵數據={row['關鍵數據']}")
            if row["重點摘要"] not in ["", " ", None] and row["published_at"].date() == date:
                contents.append(row["重點摘要"] + "\n" + str(row["關鍵數據"]))
                
                if index % 10 == 0:
                    contents.append("\n" + str(date) + "\n")
        content = "\n" + f"**{str(date)}**'s news" + "\n\n" +"\n\n".join(contents) + "\n\n" + "*"*100

        return content

    @staticmethod
    def send_email(receiver_email, content, status):

        sender_email = "taaqat93@gmail.com"
        status_mapping = {
            "succeeded": "【III】 Today's Daily Newsletter!",
            "failed": "【III】Daily Newsletter Failed to Generate"
        }

        # * Create email object *
        msg = MIMEMultipart()
        msg['From'] = "III電子報"
        msg['To'] = receiver_email
        msg['Subject'] = status_mapping[status]
        msg.attach(MIMEText(content, 'html'))

        # SMTP Config
        smtp_server = "smtp.gmail.com"
        port = 587
        password = os.getenv("SMTP")

        try:
            # 建立與伺服器的安全連線並發送電子郵件
            with smtplib.SMTP(smtp_server, port) as server:
                server.starttls()  # 開啟安全連接
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, msg.as_string())
                # print("Notification mail sent to your email address!")
        except Exception:
            print(f"Failed to send the email: {Exception}")

    # --- Post completed files back to III database
    @staticmethod
    def update_newsletter( 
            date,
            file_content, 
            expiration, 
            user_name, 
            user_email
            ):

        # form_values = '{"name": "%s", "email": %s"}'%(user_name, user_email)
        # print(form_values)
        url = 'http://61.64.60.30/news-crawler/api/file/?'

        headers = {
            'Authorization': os.getenv("III-KEY")
        }

        payload = {
            'file_name': f"Daily_newsletter-{date}.txt",
            'file_content': file_content,
            'expire_at': expiration,
            'form_values': {'name': user_name, 'email': user_email}
            }

        # *** Set up retries for the connection ***
        retry_strategy = Retry(
            total = 3,  # Number of retries
            status_forcelist = [429, 500, 502, 503, 504]  # Retry on these status codes
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter) 
        try:
            response = http.post(url, json = payload, headers = headers)
            print("Status: Successfully updated to III database.")
        except:
            response = http.post(url, json = payload, headers = headers)
            print(response.content)

        return response
    
    # --- Get generated files from III database
    @staticmethod
    def get_newsletter(
        date):
        
        url = 'http://61.64.60.30/news-crawler/api/file/?'
        
        headers = {
            'Authorization': os.getenv("III-KEY")
        }

        end_point_params = {
            'file_name': f"Daily_newsletter-{date}.txt"
        }

        response = requests.get(url, params = end_point_params, headers = headers)
        
        try:
            response = response.json()['file_content']
            return response
        except Exception as e:
            print(e)
            raise ValueError("No such file in the database!")

    # --- Update the generated content to database in google sheet
    def update_gsheet(
        date, content
    ):
        conn = st.connection("newsletter_db", type = GSheetsConnection)
        df = conn.read(worksheet = 'contents', ttl = 0)

        df_to_append = pd.DataFrame([{
            "date": date,
            "content": content
        }])

        conn.update(worksheet = 'contents', data = pd.concat([df, df_to_append], ignore_index = True))

    # --- Update the generated content to database in google sheet
    def get_user_data():
        conn = st.connection("newsletter_db", type = GSheetsConnection)
        df = conn.read(worksheet = 'users', ttl = 0)
        return df

class LlmManager:

    CLAUDE_KEY = os.getenv("CLAUDE")
    model = ChatAnthropic(model = 'claude-3-5-sonnet-20241022',
                            api_key = CLAUDE_KEY,
                            max_tokens = 8000,
                            temperature = 0.0,
                            verbose = True
                            )

    # Implement Anthropic API call 
    # *** input: chain(prompt | model), in_message(str) ***
    # *** output: json ***
    @staticmethod
    def llm_api_call(chain, in_message):

        summary_json = ""                      # initialize output value

        # This function ensures the return value from LLM is complete
        def run_with_memory(chain, in_message) -> str:
            memory = ""
            
            response = chain.invoke({"input": in_message, "memory": memory})
            while response.usage_metadata["output_tokens"] >= 5000:
                memory += response.content
                response = chain.invoke({"input": in_message, "memory": memory})
            memory += str(response.content)
            # st.write(memory)
            return memory
        
        summary_json = run_with_memory(chain, in_message)
        # st.write(summary_json)

        fail_count = 0
        while (summary_json in ["null", "DecodeError", None]):
            # While encountering error, first let Claude rest for 10 secs
            time.sleep(10)

            memory = ""

            cutting_points = [i * (len(in_message) // 2) for i in range(1, 2)]
            intermediate = [
                run_with_memory(chain, in_message[:cutting_points[0]]),
                run_with_memory(chain, in_message[cutting_points[0]:])
            ]

            response = chain.invoke({"input": "\n\n".join(intermediate), "memory": memory})
            while response.usage_metadata["output_tokens"] >= 5000:
                memory += response.content
                response = chain.invoke({"input": "\n\n".join(intermediate), "memory": memory})
            memory += str(response.content)
            summary_json = memory

            fail_count += 1

            if fail_count == 10:
                print("Claude model crushed more than 10 times during runtime. Please consider re-running...")

        return summary_json
        
    @staticmethod
    def create_prompt_chain(sys_prompt):

        # *** Create the Prompt ***
        prompt_obj = ChatPromptTemplate.from_messages(
            [
                ("system", sys_prompt),
                ("human", "{input}"),
                ("assistant", "{memory}")
            ]
        )

        # *** Create LLM Chain ***
        chain = prompt_obj | LlmManager.model

        return chain
    
prompt = """
你是一個優秀的電子報的撰文者，整理每日的新聞並統整成電子報的形式。
我會輸入一批當日的新聞資料，需要請你從這些新聞事件中幫我從 STEEP +B 六個主題（社會、科技、經濟、環境、政治、投資）中各找出 2 個最有代表性的事件。
之後，請重新命名你選出來的新聞標題，並且用 150 字簡單摘要該篇新聞。

注意事項：
1. 輸出格式請使用 HTML 格式輸出，務必不要回傳任何其他文字內容。只要回傳 html 即可！
   前面也不需要回傳 'here's the HTML format newsletter ....' 這段，只要 html 就好！
2. 若新聞 input 不夠多，則不用每個主題都寫。
3. 若新聞真的極度缺乏（ex: 只有兩三篇新聞輸入），請回傳字串 None，「不要有其他回應」。

[OUTPUT]:

'''
<!doctype html>
<html>
<head>
<meta charset='utf-8'>
<title>HTML mail</title>
</head>
<body>
    <h1>當日日期</h1>
    <section>
        <h2>社會</h2>
        <article>
            <h3>社會相關的新聞一標題</h3>
            <p>社會相關的新聞一內文</p>
        </article>
        <article>
            <h3>社會相關的新聞二標題</h3>
            <p>社會相關的新聞二內文</p>
        </article>
    </section>
    <section>
        <h2>科技</h2>
        <article>
            <h3>科技相關的新聞一標題</h3>
            <p>科技相關的新聞一內文</p>
        </article>
        <article>
            <h3>科技相關的新聞二標題</h3>
            <p>科技相關的新聞二內文</p>
        </article>
    </section>
    ...
</body>
</html>
'''


若輸入的新聞資料十分缺乏，則請回傳字串 None。
"""