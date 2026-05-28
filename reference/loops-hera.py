import os
from hera.shared import global_config
from hera.workflows import DAG, Parameter, Workflow, script
from hera.workflows.models import Sequence, ValueFrom


if __name__ == "__main__":
    global_config.host      = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token     = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    @script()
    def echo(msg: str):
        print(msg)

    # Producer that emits a JSON list as a single output parameter,
    # so a downstream step can fan out over it with `with_param`.
    @script(outputs=[Parameter(name="cities", value_from=ValueFrom(path="/tmp/cities.json"))])
    def list_cities():
        with open("/tmp/cities.json", "w") as f:
            f.write('["Paris", "Tokyo", "Lima"]')

    with Workflow(generate_name="loops-", entrypoint="dag") as w:
        with DAG(name="dag"):
            # with_items: literal list known at submit time -> one pod per item.
            literal = echo(
                name="literal",
                with_items=["alpha", "beta", "gamma"],
                arguments={"msg": "{{item}}"},
            )

            # with_sequence: numeric range -> one pod per index in [0, count).
            #     Handy when the loop is driven by a *count* rather than a list.
            ranged = echo(
                name="ranged",
                with_sequence=Sequence(count="4"),
                arguments={"msg": "index={{item}}"},
            )

            # with_param: list produced by an upstream step.
            #     The upstream JSON array is parsed by Argo; `{{item}}` is one element.
            producer = list_cities(name="producer")
            dynamic = echo(
                name="dynamic",
                with_param=producer.get_parameter("cities").value,
                arguments={"msg": "city={{item}}"},
            )
            producer >> dynamic

    w.create()
