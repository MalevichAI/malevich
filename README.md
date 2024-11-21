<div align="center">
    <h1>Malevich</h1>
    <h4>Simplest framework to built AI backend</h4>
    <a href="https://docs.malevich.ai"><img src="https://malevich-cdn.s3.amazonaws.com/github/github_readme_docs.svg"/></a>
    <a href="https://github.com/malevichAI/malevich-library"><img src="https://malevich-cdn.s3.amazonaws.com/github/github_readme_library.svg"/></a>
    <a href="https://docs.malevich.ai"><img src="https://malevich-cdn.s3.amazonaws.com/github/github_readme_try.svg"/></a>
</div>

---

Malevich is a platform for building, iterating and deploying AI-driven product backend flows:

- Vast library of pre-built components shortens the gap between the system design and first run
- You are not limited to our components: easily implement and integrate your own codebase
- Deploy on GPU and production with 1 line of code

# Getting Started

Malevich is a Python package that help you to build your components and product flows. Dive deeper into our [documentation here](https://docs.malevich.ai). Let's walkthrough how you can create an AI-based product with couple lines of code.

## Installation

To install Malevich, you should have Python 3.10 and `pip` package manager installed. Run the following command to install our package:

```bash
python3 -m pip install malevich
```

Make sure everything is set up:

```bash
malevich --help
```

## Install Components

Malevich works like a package manager allowing you to install components and use them from within the code. Let's install some components for our pipeline:

```bash
malevich install spacy openai scrape
```

Browse through our [library here](https://space.malevich.ai/workspace?tab=public&filter=app) and find the components that meet your needs.

## Connect Components

The real magic happens when components are interconnected into a more complex and useful pipeline. Let's do it by writing a few more lines of code:

```python
# flow.py

import os
import pandas as pd
from malevich import collection, flow
from malevich.openai import prompt_completion
from malevich.scrape import scrape_web
from malevich.spacy import extract_named_entities

prompt = """
You are a professional journalist. You've received
news containing many references to different people.
Your task is to understand the roles of these {entities}
and write a brief summary about them. The brief should
include the following information:

- Who is the person?
- What is their role in the news?
- What are the main events they are involved in?

Only include individuals for whom there is sufficient
information in the news. Otherwise, omit their names
entirely from the brief.
"""

@flow()
def write_brief():
   # Scrape some news from the OpenAI blog.
   links = collection(
      'News Links',
      df=pd.DataFrame(
            [
               'https://openai.com/blog/sam-altman-returns-as-ceo-openai-has-a-new-initial-board',
            ], columns=['link']
      )
   )

   # The scraper app will retrieve information from websites specified by XPath —
   # a query language that allows extracting information from markup documents.
   text = scrape_web(
      links,
      config={
         'spider': 'xpath',
         'min_length': 100,
         'max_results': 25,
         'links_are_independent': True,
         'max_depth': 1,
         'spider_cfg': {
            'components': [{
               'key': 'news-text',
               # Specify XPath query.
               'xpath': "//div[@id='content']//text()"
            }],
            'output_type': 'text',
            'include_keys': False
         }
      })

   # Extract names of entities.
   entities = extract_named_entities(
      text, config={
         'output_format': 'list',
         'filter_labels': ['PERSON'],
      }
   )

   # Write a brief about the news using OpenAI API
   # to generate text based on our prompt and extracted names.
   return text, prompt_completion(
      entities,
      config={
         'user_prompt': prompt,
         'openai_api_key': os.getenv('OPENAI_API_KEY'),
      }
   )

if __name__ == '__main__':
   from malevich import CoreInterpreter

   # Create a task for writing a brief.
   pipeline = write_brief()

   # Before running the task, interpret it to make
   # the platform aware of dependencies and execution flow.
   pipeline.interpret(
      CoreInterpreter(
         core_auth=('example', 'Welcome to Malevich!')
      )
   )

   # Execute the task.
   text, brief = pipeline()

   # Save results.
   text.get_df().to_csv('text.csv')
   brief.get_df().to_csv('brief.csv')

```

## Execute

Before executing this demo code, make sure you have the Open AI API key in your environment:

```bash
export OPENAI_API_KEY=<YOUR KEY>
```

Now, all you need is a 1 command to run the deployemnt:

```bash
python flow.py
```
Your results are stored in `text.csv` and `brief.csv` files.

## Deploy

Running this as a script is cool, but what if you want to integrate this flow into your own system with a production deployment? That's done by adding just one line:

```python
# ...

if __name__ == '__main__':
    from malevich import CoreInterpreter
    
    # Create a task for writing a brief.
    pipeline = write_brief()
    
    # Before deployment, interpret it to make
    # the platform aware of dependencies and execution flow.
    pipeline.interpret(
      CoreInterpreter(
         core_auth=('example', 'Welcome to Malevich!')
      )
    )

    # Deploy the flow
    print(pipeline.publish().get_url())
```
By running this script, you will get a link to an HTTP endpoint hosted by [Malevich Core](https://docs.malevich.ai/API/interpreter/core.html), which can be integrated into your application, be it a desktop, mobile, web app, or any other program. Enjoy the ride!
