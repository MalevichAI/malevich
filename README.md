# Malevich

Welcome to ${\textsf{\color{magenta}Malevich}}$, the innovative platform where building, iterating, and deploying machine learning-driven products is transformed into an art form. Designed for teams eager to streamline the journey from concept to production, we offers a low-code UI that enables you to craft sophisticated ML prototypes and systems with the simplicity and joy of assembling LEGO blocks. Our intuitive interface fosters collaborative brainstorming and rapid prototyping, ensuring that everyone can contribute and get up to speed in no time.

# Package

We provide you with a Python package that delivers the Command Line Interface (CLI), Malevich Square, and Malevich Meta services to your system. These services are used to build flows and apps, that will then appear in your workspace.

## Installation

The package is distributed using `pip` package manager. Run the following command to install Malevich to your system:

```bash
python3 -m pip install malevich
```

Check the setup with 

```bash
malevich --help
```

# Quick Start

## Login

To first cool thing you can do with Malevich library is deploying your flow to UI! To do it, you need firstly tell the library who you are. This is done using a file `myspace.yaml` which stores your credentials. Use the following command to login to Space:

```bash
malevich space login
```

You will be prompted for username, password and optionally for Malevich Space URL and organization id.

## Install Apps

When you install an app, you register it in your system using the `malevich.yaml` file and creating a _stub_. A stub is a generated script, that enables you to import Malevich apps as they are sub-packages of malevich. You can see, what apps are registered and available as stubs using

```bash
malevich list
```

Let us start with installing an app called **Utility**. Utility is the most basic yet powerful app provided with Malevich. It helps you to glue flows and perform operations on dataframes. To install the app, use the following command:

```bash
malevich install utility
```

Once you installed the app, it should appear as a subpackage. which means you may import a processor from it and explore it:

```python
from malevich.utility import locs
```

## Simple Flow

The simplest possible flow you may write is selecting a collumn from a collection.

```python
# myflow.py

import pandas as pd
from malevich.utility import locs
from malevich import flow, config, collection
from malevich.interpreter.space import SpaceInterpreter


@flow(reverse_id="my-first-flow", name="My First Flow")
def my_first_flow():
    """This function is a flow definition!"""
    # Create a collection
    my_collection = collection(
        df=pd.DataFrame({'left_column': ['A', 'a'], 'right_column': ['B', 'b']})
        name="My First Meta Collection"
    )
    
    # Select `right_column`
    return locs(my_collection, config(column='right_column'))

# The function returns a deployment (a.k.a task), which
# can run on Malevich Space, or Malevich Core
deployment = my_first_flow()

# Deliver flow to the platform!
deployment.interpret(SpaceInterpreter())
```
After running `python3 myflow.py` you will see a new flow named `My First Flow` in Malevich Space workspace  

# Dive Deeper

Check you the [documentation](https://docs.malevich.ai) to get an idea about other apps and possibilities of Malevich!



