import re
import json
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic

def parse_json_from_llm(json_string: str, llm: ChatOpenAI | ChatAnthropic, to_json=True) -> dict:
    """
    Parse a JSON string into a dictionary.
    """
    response = re.sub(r'```json', '', json_string)
    response = re.sub(r'```sql', '', response)
    response = re.sub(r'```', '', response).strip()
    if not to_json:
        return response
    try:
        return json.loads(response)
    except Exception as e:
        prompt = f"""
        Parse the following string into a valid JSON object.

        ```json
        {response}
        ```

        OUTPUT FORMAT:
        Return ONLY a valid JSON object with no other text. Evaluate any calculations to a number.
        """
        response = llm.invoke(prompt).content
        response = re.sub(r'```json', '', response)
        response = re.sub(r'```sql', '', response)
        response = re.sub(r'```', '', response)
        return json.loads(response)

