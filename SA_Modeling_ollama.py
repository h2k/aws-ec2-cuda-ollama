import numpy as np
import pandas as pd
import os
from tqdm import tqdm
import re
import emoji
import unicodedata
os.environ["TOKENIZERS_PARALLELISM"] = "false"
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import io
import boto3
from botocore.exceptions import BotoCoreError, ClientError



class Model_predictor:

    def remove_urls(self,text):
        # Remove URLs using regex
        url_pattern = re.compile(r'https?://\S+|www\.\S+')
        text= url_pattern.sub(r'', text)
        return text

    def has_more_than_five_chars_excluding_spaces(self,text):
        """
        This function checks if the input string has more than 5 characters, excluding spaces.
        
        :param text: The input string to check.
        :return: True if the string has more than 5 characters excluding spaces, otherwise False.
        """
        return len(text.replace(" ", "")) > 3

    def remove_mentions(self,text):
        """
        This function removes mentions (words starting with '@') from the given text.
        
        :param text: The input string containing mentions.
        :return: The string with mentions removed.
        """
        try:
            # return re.sub(r'@\w+', '', text).strip()
            # Regular expression to match mentions
            mention_pattern = r'@[\w\.\-_]+'
            # Substitute mentions with an empty string
            cleaned_comment = re.sub(mention_pattern, '', text)
            # cleaned_comment = re.sub(".", '', text)
            # Remove extra spaces left after removing mentions
            return re.sub(r'\s+', ' ', cleaned_comment).strip()
        except:
            print(text)

    def remove_special_characters(self,text):
        # Remove special characters using regex
        special_char_pattern = re.compile(r'[^\w\s\u0621-\u064A]+', re.UNICODE)
        return special_char_pattern.sub(r'', text)

    def convert_emojis(self,text):
        # Convert emojis to human-readable text
        return emoji.demojize(text, delimiters=(":", ":"))

    def normalize_unicode(self,text):
        # Normalize Unicode characters to remove control characters
        return ''.join(ch for ch in text if unicodedata.category(ch)[0] != 'C')

    def remove_end_of_lines(self,text):
        """
        This function removes end-of-line characters and replaces them with spaces.
        
        :param text: The input string containing end-of-line characters.
        :return: The string with end-of-line characters replaced by spaces.
        """
        return text.replace('\n', ' ').replace('\r', ' ')

    def clean_text(self,text):
        text = self.remove_mentions(text)
        # Remove URLs
        text = self.remove_urls(text)
        # Normalize Unicode to remove control characters
        text = self.normalize_unicode(text)
        # Convert emojis
        text = self.convert_emojis(text)
        # Remove special characters
        text = self.remove_special_characters(text)
        # Remove end of lines
        text = self.remove_end_of_lines(text)
        # Remove extra spaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def is_nan(self,x):
        try:
            return np.isnan(float(x)) 
        except ValueError:
            return False

    def process_row(self,row , processCol):
        return (
            {"Comment_pk": row["Comment_pk"], "SA": self.model_predict_SA_api(row[processCol])},
            {"Comment_pk": row["Comment_pk"], "SA": self.model_predict_comments_classification_api(row[processCol])},
        )

    def run_prediction(self,df , processCol, save_Folder_Path, save_Folder_model_topic_Path):
        predicted_SA = []
        predicted_comments = []
        df = df[df[processCol] != ""]
        rows = [row for _, row in df.iterrows()]


        with ThreadPoolExecutor(max_workers=10) as ex:
            for sa_res, com_res in tqdm(ex.map(self.process_row, rows, [processCol]*len(rows)), total=len(rows)):
               


        with ThreadPoolExecutor(max_workers=10) as ex:
            for sa_res, com_res in tqdm(ex.map(self.process_row, rows, [processCol]*len(rows)), total=len(rows)):
                predicted_SA.append(sa_res)
                predicted_comments.append(com_res)
            
        df["SA_prediction"] = predicted_SA
        df["comments_classification_prediction"] = predicted_comments
        # save the results
        if not os.path.exists(os.path.join(save_Folder_Path , save_Folder_model_topic_Path)):
            os.makedirs(os.path.join(save_Folder_Path , save_Folder_model_topic_Path))  
        df.to_csv(os.path.join(save_Folder_Path , save_Folder_model_topic_Path , "predicted_analysis.csv") , index=False) 

    #save dataframe in s3
    def save_df_to_s3(self, df, bucket_name, key, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, region_name=None):
        """
        Save a pandas DataFrame to S3 as a CSV.

        Parameters:
        - df: pandas.DataFrame to save
        - bucket_name: S3 bucket name (str)
        - key: S3 object key, e.g. "folder/filename.csv" (str)
        - aws_access_key_id, aws_secret_access_key, aws_session_token: optional AWS creds (str)
        - region_name: optional AWS region (str)

        Returns:
        - dict: {"success": True, "bucket": bucket_name, "key": key} on success
                {"success": False, "error": "<message>"} on failure
        """
        try:

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            session_kwargs = {}
            if aws_access_key_id and aws_secret_access_key:
                session_kwargs["aws_access_key_id"] = aws_access_key_id
                session_kwargs["aws_secret_access_key"] = aws_secret_access_key
                if aws_session_token:
                    session_kwargs["aws_session_token"] = aws_session_token
            if region_name:
                session_kwargs["region_name"] = region_name

            session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
            s3_client = session.client("s3")

            s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue(), ContentType="text/csv")
            return {"success": True, "bucket": bucket_name, "key": key}
        except (BotoCoreError, ClientError, Exception) as e:
            return {"success": False, "error": str(e)}
    
    # Assuming remove_mentions function is already defined
    def process_comments(self , df, comment_column):
        """
        Processes a DataFrame to check if a comment contains only mentions.

        Args:
            df (pd.DataFrame): The input DataFrame containing comments.
            comment_column (str): The name of the column with the comments.

        Returns:
            pd.DataFrame: The DataFrame with an added 'is_mentions' column.
        """
        # Apply the function to check if the cleaned comment is empty
        df[comment_column]=df[comment_column].fillna("")
        df['is_mentions_only'] = df[comment_column].apply(lambda x: self.remove_mentions(x) == '')
        return df
 
    def model_predict_SA_api(self, text , url = "http://localhost:5000/v1/api/chat"):
        # text = "أغسطس ٢٠٢٣ ، هادي المجمع والحركة فيه خفيفة رغم أني زرته بعد المغرب ، الخيارات للتسوق ليست كثيرة أعجبني فيه مقهى نصيف القريب من بوابة ٦ و ٧"
        payload = json.dumps({
        "model": "yasserrmd/ALLaM-7B-Instruct-preview",
        "messages": [
            {
            "role": "system",
            "content": "You are a precise sentiment classifier.\n\nTASK Classify the TEXT into exactly one label from LABELS.\n\nRULES\n\nChoose the single best label (no ties).\nPrefer \"Neutral\" if ambiguous (only if present).\nConsider negation, sarcasm, contrast.\nOutput MUST be one JSON object. No extra text.\nFORMAT { \"sentiment\": \"<one of the labels>\", \"confidence\": <0..1>, \"reason\": \"<max 20 words>\" }\n\nLABELS{Positive, Neutral, Negative}\n\nGUIDANCE\n\nText may be English or Arabic (or mixed).\nEmojis are sentiment clues (😍❤️👏😘🔥🌹👍🙏👌 often positive) but context dominates.\nComplaints about expensive/unreasonable prices → negative unless clearly negated.\n When you see TEXT:, classify it using the rules above. Respond ONLY with the JSON object and nothing else.\nTEXT"
            },
            {
            "role": "user",
            "content": f"TEXT:{text}"
            }
        ],
        "stream": False
        })
        headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer demo'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        # print(response.text)
        return response.text

    def model_predict_comments_classification_api(self, text , url = "http://localhost:5000/v1/api/chat"):
        payload = json.dumps({
            "model": "yasserrmd/ALLaM-7B-Instruct-preview",
            "messages": [
            {
                "role": "system",
                "content": "You are a precise text classifier.\n\nTASK\nClassify the TEXT into exactly one label from LABELS.\n\nRULES\n- Choose the single best label (no ties).\n- Prefer \"Other\" if ambiguous.\n- OUTPUT MUST BE ONLY ONE JSON OBJECT. NO EXTRA TEXT.\n\nFORMAT\n{\"label\": \"<one of the labels>\", \"confidence\": <0..1>, \"reason\": \"<max 20 words>\"}\n\nLABELS {\"Mobile App\", \"auto_loan\", \"Credit/Debit Card\", \"Loan\", \"Prizes\", \"Competition\", \"Customer Service\", \"Other\"}\n\nGUIDANCE\n- Text may be English or Arabic (or mixed).\n- If meaning unclear → label = \"Other\".\n- DO NOT write explanations outside JSON.\n\nWhen you see TEXT:, classify it using the format above.\nAfter TEXT:, reply ONLY with JSON.\n"
            },
            {
                "role": "user",
                "content": f"TEXT:{text}"
            }
            ],
            "stream": False
        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer demo'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        # print(response.text)
        return response.text



    def model_predict_posts_classification_api(self, text , url = "http://localhost:5000/v1/api/chat"):
        payload = json.dumps({
            "model": "yasserrmd/ALLaM-7B-Instruct-preview",
            "messages": [
            {
                "role": "system",
                "content": "You are a precise banking text classifier.\n\nTASK\nClassify the TEXT into exactly one label from LABELS.\n\nRULES\n- Choose the single best label (no ties).\n- Prefer \"Other\" if ambiguous.\n- OUTPUT MUST BE ONLY ONE JSON OBJECT. NO EXTRA TEXT.\n\nFORMAT\n{\"label\": \"<one of the labels>\", \"confidence\": <0..1>, \"reason\": \"<max 20 words>\"}\n\nLABELS {\"Mobile App\", \"auto_loan\", \"Credit/Debit Card\", \"Loan\", \"Prizes\", \"Competition\", \"Customer Service\", \"Other\"}\n\nGUIDANCE\n- Text may be English or Arabic (or mixed).\n- If meaning unclear → label = \"Other\".\n- DO NOT write explanations outside JSON.\n\nWhen you see TEXT:, classify it using the format above.\nAfter TEXT:, reply ONLY with JSON.\n"
            },
            {
                "role": "user",
                "content": f"TEXT:{text}"
            }
            ],
            "stream": False
        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer demo'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        # print(response.text)
        return response.text









    def classify_Posts_Topic(self,Topics,text):
        system_prompt = f"""
        You are a precise JSON generator.

        TASK
        Read the TEXT and classify it into exactly one topic. Output only one JSON object.

        RULES
        - Choose exactly one topic from the allowed list.
        - If unsure, pick the closest valid topic.
        - Text may be English, Arabic, or mixed.
        - Output MUST be valid JSON.
        - Output MUST start with '{{' and end with '}}'.
        - Do NOT include markdown, code fences, or any explanation.
        - Do NOT output any text before or after the JSON object.

        TOPICS
        {Topics}

        FORMAT
        Return exactly one JSON object with this schema:
        {{
        "topic": "<one topic only>",
        "confidence": <number between 0 and 1>,
        "reason": "<max 15 words>"
        }}

        IMPORTANT
        - No ```json fences.
        - No comments.
        - No extra text.
        - JSON only.
        """

        payload = {
            "model": "yasserrmd/ALLaM-7B-Instruct-preview",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"TEXT: \"{text}\""}
            ],
            "temperature": 0.0
        }

        headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

        response = requests.post(API_URL, json=payload, headers=headers)
        return response.json()














if __name__ == "__main__":
    filename_to_process  = "comments.csv"
    save_Folder_Path = r"processed/comment_sa/"
    bucket_name = "social-media-core-data"
    save_Folder_Path = r"AI_models1/processed"
    save_Folder_model_topic_Path = r"comment_sa"




    API_URL = "http://YOUR-ENDPOINT/v1/chat/completions"
    API_KEY = "YOUR_API_KEY"

    posts_TOPICS = [
        "Mobile App",
        "Auto Loan",
        "Credit/Debit Card",
        "Loan",
        "Prizes",
        "Competition",
        "mention other accounts",
        "follow the account"
    ]



    ai = Model_predictor()


    df = pd.read_csv(os.path.join(os.getcwd() , "AI_models",filename_to_process) , dtype = str)
    # select comments and id 
    df = df[["post_pk","Comment_pk","post_text" , "Comment_text"]]
    df["id"] = df["Comment_pk"].copy()
    # filter out the mention comments 
    df = ai.process_comments( df, comment_column="Comment_text")

    # # analize the comments
    ai.run_prediction(df ,processCol="Comment_text", save_Folder_Path= save_Folder_Path , save_Folder_model_topic_Path=save_Folder_model_topic_Path)
    # save the results in s3 
    ai.save_df_to_s3(df, bucket_name=bucket_name, key=f"{save_Folder_model_topic_Path}/predicted_analysis.csv")
