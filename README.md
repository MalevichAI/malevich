<div align="center">
    <h1>Malevich</h1>
    <h4>Your toolkit for crafting AI-powered products effortlessly</h4>
    <a href="https://docs.malevich.ai"><img src="https://malevich-cdn.s3.amazonaws.com/github/github_readme_docs.svg"/></a>
    <a href="https://github.com/malevichAI/malevich-library"><img src="https://malevich-cdn.s3.amazonaws.com/github/github_readme_library.svg"/></a>
    <a href="https://docs.malevich.ai"><img src="https://malevich-cdn.s3.amazonaws.com/github/github_readme_try.svg"/></a>
</div>

---

Malevich is an innovative platform for building, iterating and deploying AI-driven products:

- A rich set of pre-built components greatly shortens the gap between an idea and a first run.
- You are not limited to our components: easily implement and integrate your own codebase.
- Deploying on GPU in a matter of setting one parameter.

# Getting Started

Malevich offers a Python package that help you to build your apps and flows. Dive deep into our [documentation](https://docs.malevich.ai) and explore the enormous possibilities. Let's walkthrough how you can create mesmerizing products with simple lines of code.

## Installation

To install Malevich, you should have Python 3.11+ and `pip` package manager installed. Run the following command to install our package:

```bash
python3 -m pip install malevich
```

Make sure everything is set up:

```bash
malevich --help
```

## Install Apps

Malevich works like a package manager allowing you to install apps and use them from within the code. Let's install some apps that will constitute our pipeline:

```bash
malevich install spacy openai scrape
```

Our shelf has plenty to offer, browse through our [library](https://space.malevich.ai/workspace?tab=public&filter=app) and find the apps that meet your needs.

## Connect Apps

The real magic happens when apps are interconnected into a complex and useful pipeline. Let's do it by writing a few lines of code:

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

Before executing the code, make sure you have the Open AI API key in your environment:

```bash
export OPENAI_API_KEY=<YOUR KEY>
```

Now, all you need is a simple command to run the script:

```bash
python flow.py
```
Your results are stored in `text.csv` and `brief.csv` files.

## Deploy

Running scripts is cool, but what if you want to integrate this flow into your own app? That's done by adding just one line:

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
By running this script, you will get a link to an HTTP endpoint hosted by Malevich, which can be integrated into your application, be it a desktop, mobile, web app, or any other program. Enjoy the ride!
