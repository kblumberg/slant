
import time
from utils.utils import log
from utils.db import pg_load_data
from classes.GraphState import GraphState

def answer_with_sharky_preds(state: GraphState) -> GraphState:
    log('\n')
    log('='*20)
    log('\n')
    log('answer_with_sharky_preds starting...')
    # refined_query = prompt_refiner(state, 'Answer the user\'s query using the provided data.')
    refined_query = state['refined_query']
    start_time = time.time()
    # log('state:')
    # log(print_sharky_state(state))
    query = f"""
        select
        o.orderbook
        , o.collection
        , o.n_days
        , o.apy
        , p.pred

        , p.overall_value
        , p.overall_value_grade
        , p.loan_to_floor_ratio
        , p.loan_to_floor_ratio_grade
        , p.volatility
        , p.volatility_grade
        , p.repayment_history
        , p.repayment_history_grade
        , p.daily_volume
        , p.daily_volume_grade

        , p.calc_apy
        , p.offer_amount
        , p.tensor_sell
        , p.buys_7d
        , p.floor_price
        , p.adj_floor_price
        from sharky_preds p
        join sharky_orderbooks o on p.orderbook = o.orderbook
        where tensor_sell > 0
        order by overall_value desc
    """
    df = pg_load_data(query)

    # Construct prompt dynamically based on state
    prompt = f"""
        Context: You are an AI assistant analyzing NFT loan opportunities using data. This data contains key loan metrics, including APY, predicted risk, market conditions, and historical lending activity. Your goal is to assess and rank loan opportunities based on expected profitability and risk-adjusted returns.

        Data Fields:

        Orderbook (orderbook) – Unique identifier for the loan order.
        Collection (collection) – Name of the NFT collection.
        Loan Duration (n_days) – Length of the loan in days.
        APY (apy) – Stated annual percentage yield of the loan.
        Overall Value (overall_value) – The expected value of the loan.
        Overall Value Grade (overall_value_grade) – A grade for the overall value of the loan.
        Loan to Floor Ratio (loan_to_floor_ratio) – The ratio of the loan amount to the floor price.
        Loan to Floor Ratio Grade (loan_to_floor_ratio_grade) – A grade for the loan to floor ratio of the loan.
        Volatility (volatility) – The volatility of the floor price.
        Volatility Grade (volatility_grade) – A grade for the volatility of the floor price.
        Collection Repayment History (repayment_history) – The repayment history of the loan.
        Collection Repayment History Grade (repayment_history_grade) – A grade for the repayment history of the loan.
        Daily Volume (daily_volume) – The daily volume of the loan.
        Daily Volume Grade (daily_volume_grade) – A grade for the daily volume of the loan.
        Risk Probability (pred) – Probability that the floor price will drop below the offer amount (higher is riskier).
        Risk-Adjusted APY (calc_apy) – APY adjusted for risk (higher is better, negative means expected loss).
        Current Loan Offer (offer_amount) – The top loan offer amount.
        Market Sell Price (tensor_sell) – Current sell price of NFTs in this collection.
        Weekly Buys (buys_7d) – Number of NFT purchases in the last 7 days.
        Price Ratio (price_ratio) – Ratio of floor price to loan amount (lower is better).
        Floor Price (floor_price) – Current market floor price of the collection.
        Adjusted Floor Price (adj_floor_price) – Floor price adjusted for price ratio.

    
        ## **Task**
        - Answer the user's query by using the provided data.

        Best Loan Options: Rank the top loan opportunities based on expected value (expected_value).
        Risk Analysis: Identify the safest loan options based on volatility (std_volatility), repayment rate (pct_surprise_repayment_sharky), and price ratio (price_ratio).
        Profitability Assessment: Suggest whether a given loan offer is worth accepting based on the expected return (expected_value) and market activity (buys_7d, tensor_sell).

        Response Format:
        If ranking loan opportunities: Return a list of the top options with their key metrics.
        If assessing a specific loan: Provide a detailed breakdown with risk, reward, and market conditions.

        Important Considerations:

        Prioritize loans with high expected_value, low pred (risk probability), and strong repayment history (pct_surprise_repayment_sharky).
        Flag high-risk loans where expected_value is negative, std_volatility is high, or price_ratio is high.
        Consider recent market activity (buys_7d, tensor_sell) to validate loan demand.

        Do not reference any specific numbers (except for offer_amount, loan_to_floor_ratio, and floor_price), just the grades and use plain english to describe the loan opportunities.

        ## **User Query**
        **{state['query']}**

        ## **Context Data**
        ```json
        {df.to_json(orient='records')}
        ```

        ## **Response Format (Use Markdown)**
        - Use **headers** for structure (### Key Insights, ### Notable Trends, etc.).
        - Use **bullet points** for clarity.
        - Use **bold** for emphasis (e.g., **major increase**, **notable drop**; use sparingly).

        For each loan opportunity, show a "report card" with the following metrics:
        - Overall Value: A/B/C/D/F
        - Loan to Floor Ratio: A/B/C/D/F
        - Volatility: A/B/C/D/F
        - Collection Repayment History: A/B/C/D/F
        - Daily Volume: A/B/C/D/F

        Make sure to specify the collection and number of days for the loan in your response.
    """

    # Call LLM to get the decision
    answer = state['llm'].invoke(prompt).content
    time_taken = round(time.time() - start_time, 1)
    log(f'answer_with_sharky_preds finished in {time_taken} seconds')
    return {'sharky_agent_answer': answer, 'completed_tools': ["SharkyAgent"], 'upcoming_tools': ["RespondWithContext"]}