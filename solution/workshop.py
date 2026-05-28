import os
from hera.workflows import Artifact, DAG, Parameter, WorkflowTemplate, script
from hera.workflows.models import ArchiveStrategy, NoneStrategy, Sequence, ValueFrom
from hera.shared import global_config

USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"
IMAGE = "jupyter/scipy-notebook:latest"


# 1) discovery
@script(
    image=IMAGE,
    outputs=[
        Artifact(name="events", path="/tmp/events.json"),
        Parameter(name="count", value_from=ValueFrom(path="/tmp/count.txt")),
    ],
)
def list_significant(start_time: str, end_time: str, limit: int, min_magnitude: float):
    """Query USGS for the M>=min_magnitude earthquakes catalogued between
    start_time and end_time.

    Writes the full list to /tmp/events.json (exposed as the `events`
    Artifact) and the cardinality to /tmp/count.txt (exposed as the `count`
    Parameter). Downstream tasks fan out via `with_sequence(count=...)` and
    pull their own slice from the artifact -- avoiding the etcd payload
    limit that breaks `with_param` once the event list gets large.
    """
    import json
    import urllib.parse
    import urllib.request

    params = {
        "format":       "geojson",
        "starttime":    start_time,
        "endtime":      end_time,
        "minmagnitude": str(min_magnitude),
        "orderby":      "magnitude",
        "limit":        str(limit),
    }
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=60) as resp:
        payload = json.load(resp)

    events = []
    for feat in payload["features"]:
        lon, lat, depth = feat["geometry"]["coordinates"]
        events.append({
            "id":       feat["id"],
            "mag":      feat["properties"]["mag"],
            "place":    feat["properties"]["place"],
            "time_ms":  feat["properties"]["time"],
            "lon":      lon,
            "lat":      lat,
            "depth_km": depth,
        })

    with open("/tmp/events.json", "w") as f:
        json.dump(events, f)
    with open("/tmp/count.txt", "w") as f:
        f.write(str(len(events)))


# 2) per-event worker (the loop body)
@script(
    image=IMAGE,
    inputs=[Artifact(name="events", path="/tmp/events.json")],
    outputs=[
        Artifact(name="snapshot", path="/tmp/snapshot.png",
                 archive=ArchiveStrategy(none=NoneStrategy())),
        Parameter(name="summary", value_from=ValueFrom(path="/tmp/summary.json")),
    ],
)
def analyze_event(index: int):
    """For one mainshock, fetch every USGS-catalogued event within 200 km
    and the following 7 days, and render its aftershock sequence.

    The mainshock's epicenter (lat, lon) gates the spatial query and its
    origin time gates the temporal query -- so the result captures both
    productivity (how many) and decay (how fast they tail off) for that
    single event in isolation.

    Outputs:
      - /tmp/snapshot.png: two-panel figure -- magnitude-vs-hours scatter
        on the left, real GIS epicenter map (cartopy) on the right.
      - /tmp/summary.json: flat summary (id, mag, n_aftershocks,
        max_aftershock, decay_slope) exposed as the `summary` output
        parameter and consumed by the aggregator.
    """
    import subprocess
    import sys
    # cartopy is not in jupyter/scipy-notebook by default; install at runtime.
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet",
                           "cartopy"])

    import collections
    import json
    import math
    import urllib.parse
    import urllib.request
    from datetime import datetime, timedelta, timezone
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    with open("/tmp/events.json") as f:
        event = json.load(f)[index]

    event_id = event["id"]
    event_mag = event["mag"]
    event_place = event["place"]
    event_lon = event["lon"]
    event_lat = event["lat"]
    event_time_ms = event["time_ms"]

    main_time = datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
    end_time = main_time + timedelta(days=7)
    params = {
        "format":      "geojson",
        "starttime":   main_time.isoformat(),
        "endtime":     end_time.isoformat(),
        "latitude":    str(event_lat),
        "longitude":   str(event_lon),
        "maxradiuskm": "200",
        "orderby":     "time",
        "limit":       "5000",
    }
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=120) as resp:
        payload = json.load(resp)

    aftershocks = []
    for feat in payload["features"]:
        if feat["id"] == event_id:
            continue
        t = datetime.fromtimestamp(feat["properties"]["time"] / 1000, tz=timezone.utc)
        hours = (t - main_time).total_seconds() / 3600
        if hours <= 0:
            continue
        lon, lat, _ = feat["geometry"]["coordinates"]
        aftershocks.append({
            "mag":   feat["properties"]["mag"],
            "hours": hours,
            "lon":   lon,
            "lat":   lat,
        })

    slope = None
    if len(aftershocks) >= 5:
        bins = collections.Counter(int(a["hours"]) for a in aftershocks)
        xs = sorted(b for b in bins if b >= 0)
        ys = [bins[b] for b in xs]
        if len(xs) >= 3:
            lx = [math.log10(x + 1) for x in xs]
            ly = [math.log10(y) for y in ys]
            n = len(lx)
            mx = sum(lx) / n
            my = sum(ly) / n
            num = sum((lx[i] - mx) * (ly[i] - my) for i in range(n))
            den = sum((lx[i] - mx) ** 2 for i in range(n)) or 1.0
            slope = num / den

    label = "M{:.1f} -- {}".format(event_mag, event_place)
    mainshock_label = "mainshock M{:.1f}".format(event_mag)

    fig = plt.figure(figsize=(13, 5))
    ax1 = fig.add_subplot(1, 2, 1)
    if aftershocks:
        ax1.scatter([a["hours"] for a in aftershocks],
                    [a["mag"] for a in aftershocks],
                    s=20, alpha=0.6, color="tab:blue")
        ax1.axhline(event_mag, color="tab:red", linestyle="--",
                    label=mainshock_label)
        ax1.set_xlabel("Hours after mainshock")
        ax1.set_ylabel("Magnitude")
        ax1.set_title(f"Aftershocks within 200 km -- {len(aftershocks)} events")
        ax1.grid(True, alpha=0.3)
        ax1.legend(fontsize=8)
    else:
        ax1.text(0.5, 0.5, "No aftershocks in window",
                 ha="center", va="center")
        ax1.axis("off")

    # Real GIS map: PlateCarree projection centred on the mainshock
    pad = 4.0
    proj = ccrs.PlateCarree()
    ax2 = fig.add_subplot(1, 2, 2, projection=proj)
    ax2.set_extent([event_lon - pad, event_lon + pad,
                    event_lat - pad, event_lat + pad],
                   crs=proj)
    ax2.add_feature(cfeature.OCEAN, facecolor="#cfe4f5")
    ax2.add_feature(cfeature.LAND, facecolor="#f3efe6")
    ax2.add_feature(cfeature.COASTLINE, linewidth=0.6)
    ax2.add_feature(cfeature.BORDERS, linewidth=0.4, linestyle=":")
    ax2.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5,
                  color="grey")

    if aftershocks:
        ax2.scatter([a["lon"] for a in aftershocks],
                    [a["lat"] for a in aftershocks],
                    s=[max(8, a["mag"] ** 3) for a in aftershocks],
                    alpha=0.55, color="tab:blue", label="aftershocks",
                    transform=proj, zorder=3)
    ax2.scatter(event_lon, event_lat, s=280, marker="*",
                color="tab:red", edgecolors="black",
                label="mainshock", transform=proj, zorder=4)
    ax2.set_title("Epicenter map")
    ax2.legend(fontsize=8, loc="upper right")

    fig.suptitle(label)
    fig.tight_layout()
    plt.savefig("/tmp/snapshot.png", dpi=120)

    with open("/tmp/summary.json", "w") as f:
        json.dump({
            "id":             event_id,
            "mag":            event_mag,
            "place":          event_place,
            "time_ms":        event_time_ms,
            "lon":            event_lon,
            "lat":            event_lat,
            "n_aftershocks":  len(aftershocks),
            "max_aftershock": max((a["mag"] for a in aftershocks), default=None),
            "decay_slope":    slope,
        }, f)


# 3) cross-event aggregator (the fan-in)
@script(
    image=IMAGE,
    outputs=[
        Artifact(name="world_map", path="/tmp/world_map.png",
                 archive=ArchiveStrategy(none=NoneStrategy())),
    ],
)
def plot_world_map(results_json):
    """Global overview of every analyzed mainshock on a single world map.

    Marker size scales with mainshock magnitude; colour encodes the number
    of catalogued aftershocks within 200 km / 7 days. The geographic view
    tells the participant at a glance where activity clustered during the
    query window, which is much easier to read than the Bath / Omori
    scatter plots the previous step produced.
    """
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet",
                           "cartopy"])

    import json
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    # Argo aggregates each iteration's stdout into a JSON array of strings;
    if isinstance(results_json, str):
        results_json = json.loads(results_json)
    items = [json.loads(s) if isinstance(s, str) else s for s in results_json]

    proj = ccrs.Robinson()
    data_proj = ccrs.PlateCarree()
    fig = plt.figure(figsize=(13, 6.5))
    ax = fig.add_subplot(1, 1, 1, projection=proj)
    ax.set_global()
    ax.add_feature(cfeature.OCEAN, facecolor="#cfe4f5")
    ax.add_feature(cfeature.LAND, facecolor="#f3efe6")
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
    ax.gridlines(linewidth=0.3, alpha=0.4, color="grey")

    if items:
        lons = [d["lon"] for d in items]
        lats = [d["lat"] for d in items]
        mags = [d["mag"] for d in items]
        counts = [d.get("n_aftershocks", 0) for d in items]
        sizes = [max(40, (m ** 3.0)) for m in mags]

        sc = ax.scatter(lons, lats, s=sizes, c=counts,
                        cmap="plasma", alpha=0.85,
                        edgecolors="black", linewidths=0.6,
                        transform=data_proj, zorder=3)
        cbar = fig.colorbar(sc, ax=ax, orientation="horizontal",
                            shrink=0.6, pad=0.05)
        cbar.set_label("Aftershocks within 200 km / 7 d")

        top = max(items, key=lambda d: d["mag"])
        top_mag = top["mag"]
        top_place = top["place"]
        top_label = "M{:.1f} {}".format(top_mag, top_place)
        ax.annotate(top_label,
                    xy=(top["lon"], top["lat"]),
                    xycoords=data_proj._as_mpl_transform(ax),
                    xytext=(8, 8), textcoords="offset points",
                    fontsize=9, color="black",
                    bbox=dict(boxstyle="round,pad=0.2",
                              fc="white", ec="grey", alpha=0.8))

        ax.set_title("Analyzed mainshocks ({}) -- "
                     "marker size = magnitude, colour = aftershock count"
                     .format(len(items)))
    else:
        ax.set_title("No mainshocks returned by list_significant")

    fig.tight_layout()
    plt.savefig("/tmp/world_map.png", dpi=120)


# workflow definition and submission
if __name__ == "__main__":
    global_config.host      = f'https://{os.environ.get("ARGO_SERVER")}'
    global_config.token     = os.environ.get("ARGO_TOKEN")
    global_config.namespace = os.environ.get("ARGO_NAMESPACE", "argo")

    with WorkflowTemplate(
        name="quake-aftershocks",
        entrypoint="dag",
        arguments=[
            Parameter(name="start_time",    value="2026-04-28"),
            Parameter(name="end_time",      value="2026-05-28"),
            Parameter(name="limit",         value="20"),
            Parameter(name="min_magnitude", value="5.0"),
        ],
    ) as w:
        with DAG(name="dag"):
            task_list_significant = list_significant(
                arguments=[
                    Parameter(name="start_time",    value="{{workflow.parameters.start_time}}"),
                    Parameter(name="end_time",      value="{{workflow.parameters.end_time}}"),
                    Parameter(name="limit",         value="{{workflow.parameters.limit}}"),
                    Parameter(name="min_magnitude", value="{{workflow.parameters.min_magnitude}}"),
                ],
            )
            task_analyze_event = analyze_event(
                with_sequence=Sequence(
                    count=task_list_significant.get_parameter("count").value,
                ),
                arguments=[
                    Parameter(name="index", value="{{item}}"),
                    task_list_significant.get_artifact("events"),
                ],
            )
            task_plot_world_map = plot_world_map(
                arguments={"results_json": task_analyze_event.get_parameter("summary").value},
            )
            task_list_significant >> task_analyze_event >> task_plot_world_map

    w.create()
