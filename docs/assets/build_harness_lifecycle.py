import graphviz

NAVY = "#1a237e"
LAVENDER = "#e8eaf6"
AMBER = "#8d6e00"
AMBER_FILL = "#fff8e1"
GREEN = "#1b5e20"
GREEN_FILL = "#e8f5e9"
RED = "#b71c1c"
RED_FILL = "#fdecea"
SLATE = "#37474f"
GREY = "#eceff1"

g = graphviz.Digraph("harness_lifecycle", format="png")
g.attr(
    rankdir="LR",
    splines="spline",
    bgcolor="white",
    nodesep="0.55",
    ranksep="0.9",
    fontname="Helvetica",
    dpi="200",
)
g.attr("node", fontname="Helvetica", fontsize="13", shape="box", style="rounded,filled",
       penwidth="1.4", margin="0.2,0.12")
g.attr("edge", fontname="Helvetica", fontsize="11", color=SLATE, fontcolor=SLATE,
       penwidth="1.3", arrowsize="0.8")

g.node("idle", "(idle)", shape="circle", style="filled", fillcolor=GREY, color=SLATE,
       fontcolor=SLATE, width="0.9", fixedsize="true")

for name, label in [
    ("starting", "STARTING\ndocker compose up\n(core rebuilt)"),
    ("running", "RUNNING\nload gen + chaos\n(asyncio.gather)"),
    ("draining", "DRAINING\nwait for Redis\nconsumer group lag=0"),
    ("extracting", "EXTRACTING\ndocker cp delivery log\nteardown · analyze.py"),
]:
    g.node(name, label, color=NAVY, fillcolor=LAVENDER, fontcolor=NAVY)

g.node("done", "DONE", color=GREEN, fillcolor=GREEN_FILL, fontcolor=GREEN)
g.node("failed", "FAILED", color=RED, fillcolor=RED_FILL, fontcolor=RED)
g.node("cancelled", "CANCELLED", color=AMBER, fillcolor=AMBER_FILL, fontcolor=AMBER)

g.edge("idle", "starting", label="POST /test/start\n(409 if already busy)")
g.edge("starting", "running")
g.edge("running", "draining")
g.edge("draining", "extracting")
g.edge("extracting", "done")

# Cancellation: reachable from every in-flight state, all converging on the
# same cleanup path (teardown_core still runs before landing on CANCELLED).
with g.subgraph() as c:
    c.attr(rank="same")
    for src in ["starting", "running", "draining", "extracting"]:
        g.edge(src, "cancelled", label="POST /test/cancel" if src == "starting" else "",
               style="dashed", color=AMBER, fontcolor=AMBER, constraint="false")

# Failure: any unhandled exception in the same span, same teardown path.
for src in ["starting", "running", "draining", "extracting"]:
    g.edge(src, "failed", label="exception" if src == "starting" else "",
           style="dashed", color=RED, fontcolor=RED, constraint="false")

g.node("legend", "teardown_core() runs\non every exit except DONE's\n(already ran as part of EXTRACTING)",
       shape="note", style="filled", fillcolor="white", color=SLATE, fontcolor=SLATE, fontsize="10")

import os
g.render(os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness_lifecycle"),
          format="png", cleanup=True)
print("done")
