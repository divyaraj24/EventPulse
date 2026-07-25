import graphviz

NAVY = "#1a237e"
NAVY_TEXT = "#1a237e"
LAVENDER = "#e8eaf6"
GREY = "#eceff1"
RED = "#b71c1c"
RED_FILL = "#fdecea"
GREEN = "#1b5e20"
GREEN_FILL = "#e8f5e9"
AMBER = "#8d6e00"
AMBER_FILL = "#fff8e1"
SLATE = "#37474f"

g = graphviz.Digraph("architecture", format="png")
g.attr(
    rankdir="LR",
    splines="spline",
    bgcolor="white",
    nodesep="0.7",
    ranksep="0.85",
    fontname="Helvetica",
    dpi="200",
)
g.attr("node", fontname="Helvetica", fontsize="14", shape="box", style="rounded,filled",
       penwidth="1.4", margin="0.22,0.14")
g.attr("edge", fontname="Helvetica", fontsize="11.5", color=SLATE, fontcolor=SLATE,
       penwidth="1.3", arrowsize="0.8")

# ---- Experiment control plane ----
with g.subgraph(name="cluster_control") as c:
    c.attr(label="Experiment Control", fontname="Helvetica-Bold", fontsize="13",
           fontcolor=AMBER, color=AMBER, style="dashed", penwidth="1.4", labeljust="l")
    c.node("loadgen", "Load Generator\n(paced event traffic)", color=AMBER, fillcolor=AMBER_FILL, fontcolor=NAVY_TEXT)
    c.node("chaos", "Chaos Harness\nsteady → fault → recovery", color=AMBER, fillcolor=AMBER_FILL, fontcolor=NAVY_TEXT)

# ---- Ingest & durability ----
with g.subgraph(name="cluster_ingest") as c:
    c.attr(label="Ingest & Durability", fontname="Helvetica-Bold", fontsize="13",
           fontcolor=NAVY, color=NAVY, penwidth="1.2", labeljust="l")
    c.node("api", "Ingest API\n(FastAPI)", color=NAVY, fillcolor=LAVENDER, fontcolor=NAVY_TEXT)
    c.node("outbox", "Postgres\nevents + outbox\n(one transaction)", color=NAVY, fillcolor=LAVENDER, fontcolor=NAVY_TEXT)

# ---- Delivery pipeline ----
with g.subgraph(name="cluster_delivery") as c:
    c.attr(label="Delivery Pipeline", fontname="Helvetica-Bold", fontsize="13",
           fontcolor=NAVY, color=NAVY, penwidth="1.2", labeljust="l")
    c.node("relay", "Relay\n(outbox poller)", color=NAVY, fillcolor=LAVENDER, fontcolor=NAVY_TEXT)
    c.node("redis", "Redis Stream\n(consumer group)", color=NAVY, fillcolor=LAVENDER, fontcolor=NAVY_TEXT)
    c.node("worker", "Worker Pool\nbounded async concurrency",
           color=RED, fillcolor=RED_FILL, fontcolor=RED, penwidth="1.6")
    c.node("dlq", "Dead Letter\nQueue (Postgres)", color=RED, fillcolor=RED_FILL, fontcolor=RED)
    c.node("policy", "Retry Policy\nnone | naive | adaptive",
           shape="box", style="dashed", color=RED, fillcolor="white", fontcolor=RED, penwidth="1.4")

# ---- Target / chaos surface ----
with g.subgraph(name="cluster_target") as c:
    c.attr(label="Target System", fontname="Helvetica-Bold", fontsize="13",
           fontcolor=GREEN, color=GREEN, penwidth="1.2", labeljust="l")
    c.node("receiver", "Receiver Mock\nconcurrency ceiling · reject rate · latency",
           color=GREEN, fillcolor=GREEN_FILL, fontcolor=GREEN)

# ---- Results ----
with g.subgraph(name="cluster_results") as c:
    c.attr(label="Analysis", fontname="Helvetica-Bold", fontsize="13",
           fontcolor=SLATE, color=SLATE, style="dashed", penwidth="1.2", labeljust="l")
    c.node("log", "delivery_log.csv\nper-attempt outcomes", color=SLATE, fillcolor=GREY, fontcolor=SLATE)
    c.node("analyze", "analyze.py\ngoodput · latency · recovery-time", color=SLATE, fillcolor=GREY, fontcolor=SLATE)


# ---- edges ----
g.edge("loadgen", "api", label="POST /events")
g.edge("api", "outbox", label="write")
g.edge("outbox", "relay", label="poll", style="dashed")
g.edge("relay", "redis", label="XADD")
g.edge("redis", "worker", label="XREADGROUP")
g.edge("worker", "receiver", label="signed POST (HMAC-SHA256)\n⇄ 2xx / error / timeout", dir="both", arrowtail="odot")
g.edge("worker", "dlq", label="retries exhausted")
g.edge("policy", "worker", style="dashed", color=RED, arrowsize="0.7")
g.edge("chaos", "receiver", label="POST /admin/chaos\n(fault schedule)", style="dashed", color=AMBER, fontcolor=AMBER)
g.edge("worker", "log", label="log every attempt")
g.edge("log", "analyze")

import os
g.render(os.path.join(os.path.dirname(os.path.abspath(__file__)), "architecture"),
         format="png", cleanup=True)
print("done")
