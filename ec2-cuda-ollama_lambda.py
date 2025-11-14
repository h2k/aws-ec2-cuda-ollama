import json
import os
import requests

# You can override these in Lambda environment variables if you like
LLM_URL = os.environ.get("LLM_URL", "http://50.16.5.200:8080/v1/chat/completions")
LLM_MODEL = os.environ.get("LLM_MODEL", "yasserrmd/ALLaM-7B-Instruct-preview")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "demo")


def lambda_handler(event, context):
    """
    AWS Lambda handler.

    Expected event format:
    {
      "text": "أغسطس ٢٠٢٣ ، هادي المجمع والحركة فيه خفيفة رغم أني زرته بعد المغرب ، الخيارات للتسوق ليست كثيرة أعجبني فيه مقهى نصيف القريب من بوابة ٦ و ٧"
    }
    """

    # Get the text to classify from the event
    text = event.get("text")
    if not text:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing 'text' in event"}),
            "headers": {"Content-Type": "application/json"},
        }

    # System prompt (same as your original code)
    system_prompt = (
        "You are a precise sentiment classifier.\n\n"
        "TASK Classify the TEXT into exactly one label from LABELS.\n\n"
        "RULES\n\n"
        "Choose the single best label (no ties).\n"
        "Prefer \"Neutral\" if ambiguous (only if present).\n"
        "Consider negation, sarcasm, contrast.\n"
        "Output MUST be one JSON object. No extra text.\n"
        "FORMAT { \"sentiment\": \"<one of the labels>\", \"confidence\": <0..1>, \"reason\": \"<max 20 words>\" }\n\n"
        "LABELS{Positive, Neutral, Negative}\n\n"
        "GUIDANCE\n\n"
        "Text may be English or Arabic (or mixed).\n"
        "Emojis are sentiment clues (😍❤️👏😘🔥🌹👍🙏👌 often positive) but context dominates.\n"
        "Complaints about expensive/unreasonable prices → negative unless clearly negated.\n"
        "TEXT"
    )

    # Build payload (same structure as your script, but user text comes from event)
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": text,
            },
        ],
        "stream": True,  # keep same as your original code
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LLM_API_KEY}",
    }

    try:
        response = requests.post(
            LLM_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=30,
        )

        # Return raw text back to the caller (you can parse it if you want)
        return {
            "statusCode": response.status_code,
            "body": response.text,
            "headers": {"Content-Type": "application/json"},
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
            "headers": {"Content-Type": "application/json"},
        }
