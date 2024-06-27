==============
User Endpoints
==============

After logic for your application is defined with :doc:`Malevich flow </SDK/Flows/Introduction>`, you can easily integrate it to your app without a need of deploying your script anywhere. Endpoints are dedicated URLs that invoke specified logic on our clouds. Explore the feature in action:


Define a logic
--------------

Let us start with a simple example. We will define a flow that takes some textual input and returns a 
rephrased version of it. Define a flow that utilizes OpenAI's GPT-3 to rephrase a given text. 

Install an app to utilize it in your code:

.. code-block:: console
    
    malevich install openai

And then define a flow:

.. code-block:: python

    from malevich import flow, collection, CoreInterpreter
    from malevich.openai import prompt_completion

    @flow
    def rewrite_product_description():
        product_description = collection('product-description')
        return prompt_completion(
            product_description,
            model='gpt-3.5-turbo',
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            system_prompt="""
                You are SEO specialist.
            """,
            user_prompt="""
                Rewrite a raw product
                description for it to inscrease amount of sales.

                {raw_description}
            """
        )

Publish an endpoint
-------------------

Now, let's publish an endpoint that will utilize the flow we have just defined.

.. code-block:: python
    
    logic = rewrite_product_description()
    logic.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))
    endpoint = logic.publish()

.. note::

    Endpoints are defined by a unique generated hash. They are invoked by sending a POST request to the following URL:

    ``https://core.malevich.ai/api/v1/endpoints/run/<ENDPOINT_HASH>``
        

Run an endpoint
---------------

They can be invoked from any application that can send HTTP requests as well from the object directly.

.. code-block:: python

    print('Endpoint hash': endpoint.hash)
    print('Endpoint URL': endpoint.get_url())

    print(endpoint.run(
        endpoint_override=EndpointOverride(
            cfg=UserConfig(rawMapCollections={
                'product-description': [
                    {'raw_description': 'A beautiful ink pen with a comfortable grip and a smooth writing experience.'}
                ]
            })
        )
    ).results)


There is an example of running an endpoint from JavaScript:

.. code-block:: javascript

    const myHeaders = new Headers();
    myHeaders.append("Content-Type", "application/json");

    const raw = JSON.stringify({
    "cfg": {
        "rawMapCollections": {
        "product-description": [
            {
            "raw_description": "A beautiful ink pen with a comfortable grip and a smooth writing experience."
            }
        ]
        }
    }
    });

    const requestOptions = {
    method: "POST",
    headers: myHeaders,
    body: raw,
    redirect: "follow"
    };

    fetch("https://core.malevich.ai/api/v1/endpoints/run/fae7e96f288fcab9b0ff38ebdda57b87f83d66f1a2d2acd2ac39adaf54d2af91", requestOptions)
    .then((response) => response.text())
    .then((result) => console.log(result))
    .catch((error) => console.error(error));



Update an endpoint
------------------

You can update already running endpoint. It can be some minor adjustments or a complete change of the logic. To update,
you can run :meth:`publish <malevich.models.task.promised.PromisedTask.publish>` method with ``hash=`` argument. It will update the endpoint under specified hash.


.. code-block:: python

    @flow
    def new_logic():
        ...


    task = new_logic()
    task.interpret(CoreInterpreter(core_auth=('example', 'Welcome to Malevich!')))
    endpoint = task.publish(hash='...')

