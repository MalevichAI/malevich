===========
Python API
===========

Malevich provides a set of tools to make your flows and apps live. The most straightforward way is to
directly use Malevich Python API and run your flows from code. 

Deployment Assistants
----------------------

Assistants are a special type of objects included into the package to shorten the path from flow definition to its actual execution.
They take care of automatic authorization and resource management, and also provides additional interfaces to manipulate and control your flows, deployments and accounts.


Core Assistant
++++++++++++++

:class:`Core <malevich._deploy.Core>` assistant automatically authorize you on Malevich Core using credentials of your `Space <https://space.malevich.ai/>`_ account.

.. code-block:: python

    @flow
    def write_hello_letter():
        company = collection('company_info')
        return prompt_completion(
            company,
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            user_prompt="""
            Given a description of the company, write a welcome letter

            {company}
            """
        )


    if __name__ == '__main__':
        deployment = Core(
            write_hello_letter,
            user='example',                     # This can be omitted
            access_key='Welcome to Malevich!'   # if you are logged in
        )
        # Preparing task to be executed
        deployment.prepare()

        # Run with data
        deployment.run(with_logs=True, override={
            'company_info': table(
                ['Malevich AI - Platform for building AI backends'],
                columns=['company']
            )
        })

        # Get results
        print(deployment.results()[0].get_df())
        deployment.stop()
        # Get online http endpoint for deployment
        print(deployment.publish().get_url())


Space Assistant
+++++++++++++++

:class:`Space <malevich._deploy.Space>` assistant also automatically authorize you and either uploads a new version of the flow or patches the previous one. Also, the assistant can attach to existing deployments without specification of ``@flow`` decorated function or supplying a task. This makes you able to create and manipulate tasks without an access to the flow definition.

.. code-block:: python

    from malevich import Space, table

    if __name__ == '__main__':
        deployment = Space(
            reverse_id='write_hello_letter'
        )
        # Preparing task to be executed
        if deployment.get_stage().value in ['interpreted', 'stopped']:
            deployment.prepare()

        # Run with data
        deployment.run(override={
            'company_info': table(
                ['Malevich AI - Platform for building AI backends'],
                columns=['company']
            )
        })

        # Get results
        print(deployment.results()[0].get_df())
        deployment.stop()
        # Get online http endpoint for deployment
        print(deployment.publish().get_url())


