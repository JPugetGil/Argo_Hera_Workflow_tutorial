import os
from hera.shared import global_config
from hera.workflows import DAG, Parameter, Workflow, script
from hera.workflows.models import Sequence, ValueFrom


if __name__ == "__main__":
    global_config.host      = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token     = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    # 1) producer: publish a count that drives the fan-out
    @script(outputs=[Parameter(name="count", value_from=ValueFrom(path="/tmp/count.txt"))])
    def produce(n: int):
        with open("/tmp/count.txt", "w") as f:
            f.write(str(n))

    # 2) worker (the fan-out body): one pod per index, each emits a summary
    @script(outputs=[Parameter(name="summary", value_from=ValueFrom(path="/tmp/summary.json"))])
    def work(index: int):
        import json
        with open("/tmp/summary.json", "w") as f:
            json.dump({"index": index, "square": index * index}, f)

    # 3) aggregator (the fan-in): Argo collects every worker's `summary`
    #    into a JSON array of strings and hands it to one pod.
    @script()
    def aggregate(results_json):
        import json
        if isinstance(results_json, str):
            results_json = json.loads(results_json)
        items = [json.loads(s) if isinstance(s, str) else s for s in results_json]
        total = sum(it["square"] for it in items)
        print(f"received {len(items)} results, sum of squares = {total}")

    with Workflow(generate_name="fanin-fanout-", entrypoint="dag") as w:
        with DAG(name="dag"):
            p = produce(arguments={"n": 5})
            workers = work(
                with_sequence=Sequence(count=p.get_parameter("count").value),
                arguments={"index": "{{item}}"},
            )
            agg = aggregate(
                arguments={"results_json": workers.get_parameter("summary").value},
            )
            p >> workers >> agg

    w.create()
