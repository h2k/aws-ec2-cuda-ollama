from email.mime import text
import requests
import json

url = "http://localhost:5000/api/chat"


if __name__ == "__main__":
  text = "أغسطس ٢٠٢٣ ، هادي المجمع والحركة فيه خفيفة رغم أني زرته بعد المغرب ، الخيارات للتسوق ليست كثيرة أعجبني فيه مقهى نصيف القريب من بوابة ٦ و ٧"
  payload = json.dumps({
    "model": "yasserrmd/ALLaM-7B-Instruct-preview",
    "messages": [
      {
        "role": "system",
        "content": "You are a precise sentiment classifier.\n\nTASK Classify the TEXT into exactly one label from LABELS.\n\nRULES\n\nChoose the single best label (no ties).\nPrefer \"Neutral\" if ambiguous (only if present).\nConsider negation, sarcasm, contrast.\nOutput MUST be one JSON object. No extra text.\nFORMAT { \"sentiment\": \"<one of the labels>\", \"confidence\": <0..1>, \"reason\": \"<max 20 words>\" }\n\nLABELS{Positive, Neutral, Negative}\n\nGUIDANCE\n\nText may be English or Arabic (or mixed).\nEmojis are sentiment clues (😍❤️👏😘🔥🌹👍🙏👌 often positive) but context dominates.\nComplaints about expensive/unreasonable prices → negative unless clearly negated.\nTEXT"
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

  print(response.text)
