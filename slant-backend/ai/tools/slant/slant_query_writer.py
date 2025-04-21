import os
import time
from utils.utils import log
from classes.GraphState import GraphState

def slant_query_writer(state: GraphState) -> GraphState:
    """
    Writes a query to execute on a postgres database.
    Input: a question or topic you want to find information about.
    The search will return a sql query (str).
    """
    # refined_query = prompt_refiner(state, 'Write a SQL query to answer the following question.')
    refined_query = state['refined_query']
    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('slant_query_writer starting...')
    # log(f'params: {params}')
    # # Ensure params is a dictionary
    # if isinstance(params, str):
    #     try:
    #         params = json.loads(params)  # Convert JSON string to dict
    #     except json.JSONDecodeError:
    #         return "Invalid JSON input"

    # Load example queries and schema files
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct absolute paths to the files
    example_queries_path = os.path.join(current_dir, "..",  "..", "context", "example_queries.sql")
    schema_path = os.path.join(current_dir, "..", "..", "context", "schema.sql")

    with open(example_queries_path, "r") as f:
        example_queries = f.read()

    with open(schema_path, "r") as f:
        schema = f.read()

    # Create prompt template
    prompt = f"""
        You are an expert SQL query writer. Using the provided schema and example queries as reference, 
        write a SQL query to answer the following question:

        Question: {refined_query}

        Schema:
        {schema}

        Example Queries for Reference:
        {example_queries}

        Write only the SQL query without any explanation.
        The query should be formatted as a string, with no other text or formatting (no markdown, no code blocks, etc).
        Make sure to limit the number of rows returned to 10-15.
    """

    sql_query = state['llm'].invoke(prompt).content
    # log(f"Generated SQL Query: {sql_query}")
    time_taken = round(time.time() - start_time, 1)
    # log(f'slant_query_writer finished in {time_taken} seconds')
    return {'sql_query': sql_query, 'completed_tools': ["ProjectKolQueryRunner"], 'upcoming_tools': ["SlantQueryExecutor"]}

    # # Initialize model and output parser
    # model = ChatOpenAI(temperature=0)
    # output_parser = StrOutputParser()

    # # Create chain
    # chain = prompt | model | output_parser

    # # Execute chain
    # sql_query = chain.invoke({
    #     "question": question,
    #     "schema": schema,
    #     "example_queries": example_queries
    # })


    # return sql_query