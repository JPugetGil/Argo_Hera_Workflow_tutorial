import os
from hera.shared import global_config
from hera.workflows import Workflow, script, DAG


if __name__ == "__main__":
    global_config.host      = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token     = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    @script()
    def echo(msg: str):
        print(msg)

    with Workflow(generate_name="diamond-", entrypoint="diamond") as w:
        with DAG(name="diamond"):
            a = echo(name="A", arguments={"msg": "A"})
            b = echo(name="B", arguments={"msg": "B"})
            c = echo(name="C", arguments={"msg": "C"})
            d = echo(name="D", arguments={"msg": "D"})
            a >> [b, c] >> d

    w.create()