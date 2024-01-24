===============
Malevich
===============


Getting Started
===============

Welcome to `Malevich <https://malevich.ai>`_ - a platform for building ML-driven prototypes 
and iterating them to production. This page contains a brief overview of the platform capabilities
that could be utilized from Python code or command-line interface.

Explore more about `building apps <Apps/Building_Apps.html>`_ and `assembling flows <Flows/Assembling_Flows.html>`_ to
start developing on Malevich. Check out `API reference <API/index.html>`_ to get insight into code details.

If you with to contribute to the Malevich package, please refer to `Contributing <Contributing.html>`_ page.


Installation
===============

Malevich provides various tools for interacting with the platform. One of them
is a :code:`malevich` Python package, that includes `Malevich Square <API/square.html>`_,
CLI, Metascript, CI and other minor tools. The package is distributed via PyPI, so 
you can install it with :code:`pip`:

.. code-block:: bash

    pip install malevich


Implement Your Idea
===================

Imagine you have a brilliant idea of a product, and you need to utilize a number
of services like Open AI or make an inferenece on a pre-trained model from Hugging Face, or
solve a common NLP task with SpaCy? With Malevich, you can turn the idea into 
working prototype with just a few steps and ridiculously small amount of code.

Let's make it real! Firstly, you have to install apps:

.. code-block:: bash

    malevich install spacy openai


You may see :code:`malevich.yaml` and :code:`malevich.secret.yaml` files appeared in your
current directory. Like any other package system, like :code:`pip` or :code:`npm`, Malevich
keep track of installed components to make your environment reproducible. 

Once you installed apps, you can start weaving them into a flow. Let's create a file
:code:`flow.py` with the following content:

.. code-block:: python

   import os

   import pandas as pd

   # Metascript
   from malevich import collection, flow

   # Malevich Apps: OpenAI, Scrape, Spacy
   from malevich.openai import prompt_completion
   from malevich.scrape import scrape_web
   from malevich.spacy import extract_named_entities

   prompt = """
   You are a professional journalist. You got
   a piece of news that contains a lot of
   references to different people. You need to
   undestand the role of {entities} and write
   a short brief about it. The brief should
   contain the following information:

   - Who is the person?
   - What is the role of the person in the news?
   - What are the main events that the person is
   involved in?

   Include only people for whom there is enough
   information in the news. Otherwise, exclude their
   name from the brief totally.
   """


   @flow()
   def write_brief():
      # Let's scrape some news
      # from OpenAI blog
      links = collection(
         'News Links',
         df=pd.DataFrame(
               [
                  'https://openai.com/blog/sam-altman-returns-as-ceo-openai-has-a-new-initial-board',
               ], columns=['link']
         )
      )

      # Scraper app will retrieve
      # information from a website
      # specified by xPath -- a
      # query language that allows
      # to extract information from
      # markup documents
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
                     # Specify xPath query
                     'xpath': "//div[@id='content']//text()"
                  }],
                  'output_type': 'text',
                  'include_keys': False
               }
         })

      # Now let's extract names
      entities = extract_named_entities(
         text, config={
               'output_format': 'list',
               'filter_labels': ['PERSON'],
         }
      )

      # And finally, let's write a brief
      # about the news. We will use OpenAI
      # API to generate a text based on
      # the prompt and extracted people
      # names
      return text, prompt_completion(
         entities,
         config={
               'user_prompt': prompt,
               'openai_api_key': os.getenv('OPENAI_API_KEY'),
         }
      )


   if __name__ == '__main__':
      from malevich import CoreInterpreter

      # Creating a task of writing a brief
      pipeline = write_brief()

      # Before we can run the task, we need
      # to interpret it, which means to
      # make the platform aware of the
      # dependencies and execution flow
      pipeline.interpret(
         CoreInterpreter(
               core_auth=('example', 'Welcome to Malevich!')
         )
      )

      # Running an actual task
      text, brief = pipeline()

      # Saving results
      text.get_df().to_csv('text.csv')
      brief.get_df().to_csv('brief.csv')

As you might see, solving a task of extracting news, retrieving people names and
writing a brief about it is a matter of a proper configuration of three apps. Once you run
the pipeline, you will see files :code:`text.csv` and :code:`brief.csv` in your current
directory and can observe results.

Run the flow with the following command (make sure you have :code:`OPENAI_API_KEY` environment variable set):

.. code-block:: bash

      python flow.py


Make Your Own Apps
==================

We are steadily working on expanding the list of available apps, and if you 
find that you need something we have not yet finished, you can implement it and 
assemble flows with your custom apps. You might integrate the whole codebase
into Malevich and use the platform as your infra to deploy and show your products.


.. toctree::
   :maxdepth: 3
   :caption: Contents:

   Getting Started <self>
   Apps/index
   API/index