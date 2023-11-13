import os
from malevich.openai import chat_with_openai_using_prompts
from malevich.utility import locs
from malevich import flow, config
import pandas as pd

@flow(
    reverse_id="example.flow.openai.simple_greeting", 
    name="Simple New Employee Greeting with OpenAI",
    dfs_are_collections=True
)
def simple_greeting_flow(names: pd.DataFrame):
    """Writes a letter with greetings for new employee"""
    openai_config = config(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        system_prompt="You a manager's assistant. "
        "Your task is to write a greeting letter to a new employee using "
        "given information",
        user_prompt="The name of the employee is {name}. Be polite and kind",
        max_tokens=512,
        n=3,
    )
    
    answers = chat_with_openai_using_prompts(
        variables=names,
        config=openai_config
    )
    
    # Select only 'content' column 
    # with `locs` operator
    return locs(answers, config(column="content")) 
