from .openai_api.simple_greeting import simple_greeting_flow

flows = [
    (simple_greeting_flow, {'names': {'name': ['Alex Teexone']}})
]