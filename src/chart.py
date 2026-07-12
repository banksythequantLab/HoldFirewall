"""Bar chart: held documents destroyed, naive vs SERIALIZABLE (from live run)."""
import json, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

r = json.load(open(r"B:\HoldFirewall\docs\spoliation_result.json"))
vals = [r["naive"], r["serializable"]]
labels = ["Naive\ncheck-then-delete", "CockroachDB\nSERIALIZABLE"]
colors = ["#f85149", "#3fb950"]

plt.rcParams.update({"figure.facecolor":"#0d1117","axes.facecolor":"#0d1117",
    "text.color":"#e6edf3","axes.labelcolor":"#e6edf3",
    "xtick.color":"#e6edf3","ytick.color":"#8b949e","font.size":12})
fig, ax = plt.subplots(figsize=(7.6,5.2))
bars = ax.bar(labels, vals, color=colors, width=0.55)
for b,v in zip(bars, vals):
    ax.text(b.get_x()+b.get_width()/2, v+max(vals)*0.02+0.3, str(v),
            ha="center", va="bottom", fontsize=22, fontweight="bold",
            color="#e6edf3")
ax.set_ylabel("held documents destroyed (spoliation)")
ax.set_title(f"Hold Firewall: {r['deleters']} deleters racing a hold on {r['docs']} docs",
             color="#e6edf3", fontsize=13, pad=12)
ax.set_ylim(0, max(vals)*1.18+1)
ax.grid(True, axis="y", color="#21262d", lw=0.6)
out = r"B:\HoldFirewall\docs\spoliation.png"
plt.tight_layout(); plt.savefig(out, dpi=140); print("wrote", out)
