import json
import time
from utils.utils import log
from classes.GraphState import GraphState

def validate_and_clean_highcharts_json(config_str: str) -> str:
    """
    Ensures the JSON string is valid and all expressions are evaluated.
    """
    try:
        parsed = json.loads(config_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from LLM: {e}")

    # Optional: Check recursively that no string contains expressions like "100 - 57.3"
    def contains_expression(value):
        if isinstance(value, str) and any(op in value for op in ['+', '-', '/']):
            return True
        elif isinstance(value, list):
            return any(contains_expression(v) for v in value)
        elif isinstance(value, dict):
            return any(contains_expression(v) for v in value.values())
        return False

    # if contains_expression(parsed):
    #     print(parsed)
    #     raise ValueError("Highcharts JSON contains unevaluated expressions.")

    return json.dumps(parsed)

def format_for_highcharts(state: GraphState) -> GraphState:
    """
        Formats a data frame for Highcharts.
        Input:
            a dictionary with the following keys:
                - df: a data frame
                - question: a question provided by the user (str)
        Returns:
            a dictionary with the following keys:
                - highcharts_config: a json object of the highcharts config
    """
    start_time = time.time()
    log('\n')
    log('='*20)
    log('\n')
    log('format_for_highcharts starting...')
    if state['flipside_sql_query_result'].empty:
        log('flipside_sql_query_result is empty')
        return {'highcharts_config': None, 'completed_tools': ["FormatForHighcharts"]}
    # log('state:')
    # log(print_sharky_state(sharkyState))

    # # Ensure params is a dictionary
    # if isinstance(params, str):
    #     try:
    #         params = json.loads(params)
    #     except json.JSONDecodeError:
    #         return "Invalid JSON input"

    # state = {
    #     'flipside_sql_query_result': 'this is the query result',
    #     'query': 'this is the query'
    # }

    # Create prompt template
    prompt = """
You are an expert data analyst and Highcharts visualization specialist. Your task is to create a well-structured Highcharts JSON configuration object based on the provided data.

---

### **Task**
Using the provided dataset, generate a **fully functional Highcharts configuration object** that accurately represents the user's requested chart.

---

### **Inputs**
- **Data**: {sql_query_result}
- **User Question**: {question}

---

### **Requirements**
1. **Follow Highcharts Best Practices**
   - Choose an appropriate chart type (e.g., line, bar, pie) based on the data and question.
   - Ensure the axes, labels, and tooltips are well formatted and readable.
   - Add proper titles and legends when necessary.
   - Avoid using gridlines for the x-axis and y-axis. (gridLineWidth: 0 for xAxis and yAxis)
   - Make sure to always include 0 in the y-axis.

2. **Use the Following Colors**
   - **Primary Color**: `#1373eb`
   - **Secondary Color**: `#ffffff`
   - **Tertiary Color**: `#ffe270`
   - **Background Color**: transparent
   - **Titles Color**: `#FFFFFF`
   - **Text Color**: `#FFFFFF`
   - **Axis Line and Label and Tick Color**: `#FFFFFF`
   - **Axis Font Size**: `12px`
   - **Hover Background Color**: `#FFFFFF`
   - **Hover Text Color**: `#1060c9`
   - **Others Colors**: If there are more than 3 series, use other different hues of the primary color (both light and dark all the way to white/black).

3. **Handle Missing or Incomplete Data Gracefully**
   - If the dataset contains null or missing values, ensure they are handled in a way that does not break the chart.
   - If the question is unclear, make a reasonable assumption and document it in a comment within the JSON.

4. **Output Format**
    - **Return ONLY a valid Highcharts JSON configuration object**.
    - **No markdown, no code blocks, no additional text**.
    - **Do NOT include any function calls or JS code**.
    - **Use label.format instead of "formatter: function()"**
    - **Do NOT use any Date functions (e.g. Date.UTC, Date.parse, etc.)**
    - **This will be passed to a JSON.parse() function, so it must be valid JSON with only strings, no functions**
    - Ensure the JSON is properly formatted and structured.
    - All values in the JSON must be **fully evaluated and concrete**.
    - Do **NOT** include expressions like `100 - 57.372558`. Compute it first, and write the final number (e.g. `42.627442`).
    - The chart JSON must be valid when passed to `JSON.parse()` in JavaScript — it should NOT contain any operations, expressions, or function calls.
    - In the "series" section, use empty `data: []` arrays. Data will be filled in later.
    - If the x-axis represents time, use "xAxis": {{ "type": "datetime" }} and expect series.data to be an array of {{ x: timestamp_ms, y: value }} and the column name for the x-axis is `timestamp`.
    - If the x-axis is categorical, use "xAxis": {{ "categories": [] }} and expect series.data to be an array of numbers and the column name for the x-axis is `category`.

---

### **Expected JSON Structure**
{{
    "chart": {{ "type": "appropriate_chart_type" }},
    "title": {{ "text": "Descriptive Title" }},
    "xAxis": {{ "categories": ["Category1", "Category2", ...] OR "type": "datetime" depending on the data }},
    "yAxis": {{ "title": {{ "text": "Y-Axis Label" }} }},
    "series": [
        {{
            "name": "Series Label",
            "data": [],
            "column": "COLUMN_NAME_TO_FILL",
            "color": "COLOR_TO_BE_FILLED"
        }}
    ]
}}
""".format(sql_query_result=state['flipside_sql_query_result'], question=state['query'])


    # print('prompt')
    # print(prompt)

    # Initialize model and output parser
    # llm = ChatOpenAI(
    #     model="gpt-4o",
    #     openai_api_key=OPENAI_API_KEY,
    #     temperature=0.0
    # )
    raw_config = state['sql_llm'].invoke(prompt).content
    highcharts_config = validate_and_clean_highcharts_json(raw_config)
    log('format_for_highcharts highcharts_config')
    log(highcharts_config)
    time_taken = round(time.time() - start_time, 1)
    log(f'format_for_highcharts finished in {time_taken} seconds')
    # print(f"highcharts_config: {highcharts_config}")

    return {'highcharts_config': highcharts_config, 'completed_tools': ["FormatForHighcharts"], 'upcoming_tools': ["AnswerWithContext"]}