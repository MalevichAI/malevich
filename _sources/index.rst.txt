===============
Malevich
===============

Getting Started
===============

Welcome to `Malevich <https://malevich.ai>`_ — a platform for building ML-driven prototypes and iterating them to production. This page provides a brief overview of the platform's capabilities that can be utilized from Python code or the command-line interface.

Explore more about :doc:`building apps </SDK/Apps/Building>` and :doc:`assembling flows </SDK/Flows/Introduction>` to start developing on Malevich. Check out the :doc:`API reference <API/index>` for detailed insights into code functionalities.

If you wish to contribute to the Malevich package, please refer to the :doc:`Contributing <Community/Contributing>` page.

Installation
===============

Malevich offers various tools for interacting with the platform, including the :code:`malevich` Python package. This package encompasses :doc:`Malevich Square <API/square/index>`, CLI, Metascript, CI, and other minor tools. It is distributed via PyPI, allowing you to install it with :code:`pip`:

.. code-block:: bash

    pip install malevich

Implement Your Idea
===================

Imagine having a brilliant product idea that requires utilizing services like OpenAI, making inferences on a pre-trained model from Hugging Face, or solving a common NLP task with SpaCy. With Malevich, you can turn your idea into a working prototype with just a few steps and an impressively small amount of code.

Let's make it real! First, you need to install apps:

.. code-block:: bash

    malevich install spacy openai

Following this, you may notice :code:`malevich.yaml` and :code:`malevich.secret.yaml` files appearing in your current directory. Similar to other package systems like :code:`pip` or :code:`npm`, Malevich keeps track of installed components to make your environment reproducible.

Once the apps are installed, you can begin integrating them into a flow. Create a file named :code:`flow.py` with the following content:

.. code-block:: python

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
         openai_api_key=os.getenv('OPENAI_API_KEY'),
         user_prompt=prompt
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

      # Prepare task.
      pipeline.prepare()
    
      # Execute the task.
      pipeline.run()

      # Save results.
      text, brief = pipeline.results()
      text.get_df().to_csv('text.csv')
      brief.get_df().to_csv('brief.csv')

As you can see, solving the task of extracting news, identifying people's names, and writing a brief is simply a matter of configuring three apps correctly. Once you run the pipeline, you will find :code:`text.csv` and :code:`brief.csv` files in your current directory and can review the results.

Run the flow with this command (ensure your :code:`OPENAI_API_KEY` environment variable is set):

.. code-block:: bash

    python flow.py

Make Your Own Apps
==================

We are continually expanding our list of available apps. If you find something missing that you need, we provide
all the tools to create your own apps and optionally share them with the community. See :doc:`/SDK/Apps/Building` for more details.


.. toctree::
   :hidden:
   :maxdepth: 3
   :caption: Contents:

   Getting Started <self>
   SDK/index
   API/index
   Community/index
