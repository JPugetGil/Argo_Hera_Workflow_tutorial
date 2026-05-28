import os
from hera.shared import global_config
from hera.workflows import Workflow, script


if __name__ == "__main__":
    global_config.host      = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token     = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    @script()
    def hello(name: str):
        print(f"Hello, {name}, from Hera!")

    with Workflow(
        generate_name="hello-hera-",
        entrypoint="hello",
        arguments={"name": "world"},
    ) as w:
        hello()

    w.create()