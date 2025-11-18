import requests
import json

url = "http://localhost:5000/api/chat"


if __name__ == "__main__":
  text = "مبروك حق الفائز"
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

  print(response.text)
