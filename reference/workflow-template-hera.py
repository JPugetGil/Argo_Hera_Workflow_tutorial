import os
from hera.shared import global_config
from hera.workflows import Parameter, WorkflowTemplate, script


if __name__ == "__main__":
    global_config.host      = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token     = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    @script()
    def greet(name: str, greeting: str):
        print(f"{greeting}, {name}!")

    # A WorkflowTemplate is registered once and re-submitted with different parameters
    with WorkflowTemplate(
        name="greet-template",
        entrypoint="greet",
        arguments=[
            Parameter(name="name",     value="world"),
            Parameter(name="greeting", value="Hello"),
        ],
    ) as wt:
        greet(
            arguments=[
                Parameter(name="name",     value="{{workflow.parameters.name}}"),
                Parameter(name="greeting", value="{{workflow.parameters.greeting}}"),
            ],
        )

    wt.create()
