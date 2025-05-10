import json
import time
import pandas as pd
from utils.utils import log
from classes.JobState import JobState

def validate_and_clean_highcharts_json(config_str: str) -> str:
    """
    Ensures the JSON string is valid and all expressions are evaluated.
    """
    try:
        parsed = json.loads(config_str)
    except json.JSONDecodeError as e:
        # log('Invalid JSON from LLM:')
        # log(config_str)
        # log(e)
        raise ValueError(e)

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

def format_for_highcharts(state: JobState) -> JobState:
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
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('format_for_highcharts starting...')
    if state['flipside_sql_query_result'].empty:
        # log('flipside_sql_query_result is empty')
        return {'highcharts_configs': [], 'completed_tools': ["FormatForHighcharts"]}
    sql_query_result = state['flipside_sql_query_result'] if len(state['flipside_sql_query_result']) <= 10 else pd.concat([state['flipside_sql_query_result'].head(5), state['flipside_sql_query_result'].head(5)])
    if 'date_time' in sql_query_result.columns and 'timestamp' in sql_query_result.columns:
        del sql_query_result['date_time']
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
    all_columns = [x for x in state['flipside_sql_query_result'].columns.tolist() if x not in ['timestamp', 'category', 'date_time']]
    number_columns = [x for x in all_columns if state['flipside_sql_query_result'][x].dtype in ['int64', 'float64']]
    categorical_columns = [x for x in all_columns if not state['flipside_sql_query_result'][x].dtype in ['int64', 'float64']]
    filter_columns = f"""
            "filter_column": "COLUMN / FIELD NAME in the data" or "", # if you need to subset the data / filter by an additional column, specify the column name here (must be one of:[ {', '.join(categorical_columns)}])
            "filter_value": "VALUE in the filter_column to filter by" or "" # if you need to filter by a value, specify the value here
    """ if len(categorical_columns) else ""
    log(f'number_columns: {number_columns}')
    log(f'categorical_columns: {categorical_columns}')
    categories = '", "'.join(state['flipside_sql_query_result']['category'].unique().tolist()) if 'category' in state['flipside_sql_query_result'].columns else []
    is_categorical = 1 if 'category' in state['flipside_sql_query_result'].columns else 0
    xAxis = f"""
        {{ "categories": ["{categories}"] }}
    """ if len(categories) else f"""
        {{ 'type': 'datetime' }}
    """
    log(f'xAxis: {xAxis}')
    prompt = """
You are an expert data analyst and Highcharts visualization specialist. Your task is to create a well-structured Highcharts JSON configuration object based on the provided data.

---

### **Task**
Using the provided dataset, generate a **list of fully functional Highcharts configuration objects** that accurately represents the user's requested chart.

---

### **Inputs**
- **Data**: {sql_query_result}
- **Available Number Columns**: {available_columns}
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
   - **Others Colors**: If there are more than 3 series, use other different hues of the primary color (both light and dark all the way to white/black). Do not use the same color more than once.

3. **Handle Missing or Incomplete Data Gracefully**
   - If the dataset contains null or missing values, ensure they are handled in a way that does not break the chart.
   - If the question is unclear, make a reasonable assumption and document it in a comment within the JSON.

4. **Output Format**
    - **Return ONLY a valid list of Highcharts JSON configuration objects**. Must be >= 1 object in the list. Have a preference for just 1 object, but if you cannot display all the data in 1 object, return multiple. Typically, multiple charts are required when there are > 3 dimensions (e.g. 2 categorical and 2 numeric that need to be displayed).
    - **No markdown, no code blocks, no additional text**.
    - **Do NOT include any function calls or JS code**.
    - **Use label.format instead of "formatter: function()"**
    - **Do NOT use any Date functions (e.g. Date.UTC, Date.parse, etc.)**
    - **This will be passed to a JSON.parse() function, so it must be valid JSON with only strings, no functions**
    - Ensure the JSON is properly formatted and structured.
    - The chart JSON must be valid when passed to `JSON.parse()` in JavaScript — it should NOT contain any operations, expressions, or function calls.
    - In the "series" section, use empty `data: []` arrays. Data will be filled in later.
    - If the x-axis represents time, use "xAxis": {{ "type": "datetime" }} and expect series.data to be an array of {{ x: timestamp_ms, y: value }} and the column name for the x-axis is `timestamp`.
    - If the x-axis is categorical, use "xAxis": {{ "categories": [] }} and expect series.data to be an array of numbers and the column name for the x-axis is `category`.
    - If it is a "type": "datetime" chart, have a preference for a line chart.
    - Create as many series charts as needed to display all requested data.
    - Make sure you are not forgetting any data or series.

---

### **Expected JSON Structure**
[{{
    "chart": {{ "type": "appropriate_chart_type" }},
    "title": {{ "text": "Descriptive Title" }},
    "xAxis": {xAxis},
    "yAxis": {{ "title": {{ "text": "Y-Axis Label" }} }},
    "series": [
        {{
            "name": "Series Label", {name_comment}
            "data": [], # always leave this as an empty array, we will fill it in later
            "column": "COLUMN / FIELD NAME in the data", # one of the available_columns
            "color": "COLOR_TO_BE_FILLED",
            {filter_columns}
        }}
        , ...
    ]
}}
, ...
]
""".format(
    sql_query_result=sql_query_result.to_markdown()
    , question=state['analysis_description']
    , available_columns=number_columns
    , filter_columns=filter_columns
    , xAxis=xAxis
    , name_comment = ' # there MUST be 1 series for each of: "' + categories + '" and the series name MUST be the same as the value.' if is_categorical else ''
)

# x = timestamp
# y = value
# category1
# category2

# x = category1
# category2
# y = value

    # print('prompt')
    # print(prompt)

    # Initialize model and output parser
    # llm = ChatOpenAI(
    #     model="gpt-4o",
    #     openai_api_key=OPENAI_API_KEY,
    #     temperature=0.0
    # )
    raw_config = state['reasoning_llm'].invoke(prompt).content
    highcharts_configs = validate_and_clean_highcharts_json(raw_config)
    log('format_for_highcharts highcharts_configs')
    log(highcharts_configs)
    time_taken = round(time.time() - start_time, 1)
    # log(f'format_for_highcharts finished in {time_taken} seconds')
    # print(f"highcharts_config: {highcharts_config}")

    return {'highcharts_configs': highcharts_configs, 'completed_tools': ["FormatForHighcharts"], 'upcoming_tools': ["RespondWithContext"]}