"""Asset definitions, kept out of the DAG files that produce and consume them.

A DAG says what it publishes and what it waits for by importing a name from
here, never by repeating an identifier string. Two DAGs that disagree by one
character would otherwise be talking about two different assets, and the only
symptom would be a downstream DAG that never runs.
"""
