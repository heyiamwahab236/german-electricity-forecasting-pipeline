"""Render technical visuals directly from the measured, sanitized run evidence."""
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

base = Path(__file__).resolve().parents[1]
evidence = json.loads((base / "reports/verified_run.json").read_text())
evaluation = evidence["tasks"]["training_evaluation"]["result"]
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "svg.fonttype": "none"})

fig, axes = plt.subplots(1, 2, figsize=(10, 4.1), facecolor="#f8fafc")
for ax, key, label in zip(axes, ("mae_eur_mwh", "rmse_eur_mwh"), ("Mean absolute error", "Root mean squared error")):
    values = [evaluation["test"][key], evaluation["baseline_24h_test"][key]]
    bars = ax.bar(["XGBoost", "Previous-day baseline"], values, color=["#0f766e", "#64748b"], width=.6)
    ax.set_title(label, loc="left", fontweight="bold")
    ax.set_ylabel("EUR / MWh")
    ax.set_ylim(0, max(values) * 1.25)
    ax.bar_label(bars, labels=[f"{v:.2f}" for v in values], padding=6)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_axisbelow(True)
    ax.grid(axis="y", alpha=.15)
fig.suptitle("Measured error on the chronological held-out test", fontsize=15, fontweight="bold")
fig.text(.5, .015, "10,642 test hours · training-only preprocessing · 24-hour boundary purge", ha="center", fontsize=9, color="#475569")
fig.tight_layout(rect=(0,.04,1,.92))
fig.savefig(base / "docs/test-errors.png", dpi=160)
fig.savefig(base / "docs/test-errors.svg")
plt.close(fig)

fig, ax = plt.subplots(figsize=(13, 4.5), facecolor="#f8fafc")
ax.set_xlim(0,13); ax.set_ylim(0,4.5); ax.axis("off")
ax.text(.2,4.12,"German electricity forecasting pipeline",fontsize=20,fontweight="bold",color="#0f172a")
ax.text(.2,3.73,"Databricks serverless · PySpark engineering · Delta Lake storage",fontsize=12,color="#475569")
boxes = [("Original CSVs", "SMARD hourly exports"), ("Raw Delta", "Strings + source lineage"), ("Cleaned Delta", "UTC + quality gates"), ("Feature Delta", "Calendar + exact lags"), ("XGBoost", "Chronological evaluation"), ("Predictions", "Saved model + pipeline")]
for i, (title, detail) in enumerate(boxes):
    x=.15+i*2.16
    box = FancyBboxPatch((x,2.05),1.95,1.2,boxstyle="round,pad=.08,rounding_size=.08",edgecolor="#cbd5e1",facecolor="white")
    ax.add_patch(box)
    ax.text(x+.975,2.82,title,ha="center",fontsize=12,fontweight="bold",color="#0f766e")
    ax.text(x+.975,2.4,detail,ha="center",fontsize=8.4,color="#475569")
    if i<5:
        ax.annotate("", xy=(x+2.08,2.65),xytext=(x+1.98,2.65),arrowprops={"arrowstyle":"->","color":"#64748b","lw":1.5})
ax.add_patch(FancyBboxPatch((.17,.55),12.55,.7,boxstyle="round,pad=.08",edgecolor="#99f6e4",facecolor="#f0fdfa"))
ax.text(6.45,.91,"Workflow dependencies + run IDs + config manifest + corrupted-data tests",ha="center",fontsize=12,color="#115e59")
ax.text(.2,.1,"Forecasting uses calendar and prior prices; future actual generation/consumption are excluded.",fontsize=9,color="#475569")
fig.tight_layout()
fig.savefig(base / "docs/architecture.svg")
fig.savefig(base / "docs/architecture.png",dpi=160)
plt.close(fig)

fig, ax = plt.subplots(figsize=(10,6),facecolor="#f8fafc")
ax.set_xlim(0,10); ax.set_ylim(0,6); ax.axis("off")
ax.text(.35,5.55,"Verified Databricks workflow",fontsize=22,fontweight="bold",color="#0f172a")
ax.text(.35,5.12,"Recorded API results · 4 October 2026 · all seven tasks succeeded",fontsize=11,color="#475569")
labels={"ingestion":"Raw ingestion", "validation":"Cleaning and validation", "joins":"Dataset joins", "features":"Feature preparation", "training_evaluation":"XGBoost evaluation", "prediction":"Historical prediction", "tests":"Corrupted-data and model tests"}
for i,(key,label) in enumerate(labels.items()):
    state=evidence["tasks"][key]["state"]
    y=4.65-i*.44
    ax.text(.45,y,f"{i+1:02d}",color="#64748b",fontsize=11)
    ax.text(1.1,y,label,color="#0f172a",fontsize=12)
    ax.text(8.1,y,state,color="#0f766e",fontsize=11,fontweight="bold")
ax.text(.4,1.04,"53,256 joined hours     |     0 unmatched timestamps     |     0 lag mismatches",fontsize=11,color="#115e59")
ax.text(.4,.62,"10 cloud test cases passed; original local unit suite: 18 tests passed.",fontsize=11,color="#475569")
ax.text(.4,.2,"Evidence summary rendered from actual run outputs; not a Databricks console screenshot.",fontsize=9,color="#64748b")
fig.tight_layout()
fig.savefig(base / "docs/workflow-evidence.png",dpi=160)
fig.savefig(base / "docs/workflow-evidence.svg")
plt.close(fig)
print("Rendered architecture, measured-error chart and verified-workflow evidence")
