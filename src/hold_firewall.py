"""Dynamic Hold Firewall -- a real-time spoliation guard on CockroachDB.

The moment a litigation hold is triggered, an agent must lock down responsive
documents WHILE employees and automated retention/cron jobs are still deleting
data. That is a concurrency problem: between a deleter checking "is this on
hold?" and performing the delete, the hold can land. Under a naive
read-then-write, the delete slips through and evidence is destroyed --
spoliation, and a sanctionable one.

CockroachDB SERIALIZABLE makes the check-and-delete atomic against the
hold-placement write: the conflict is detected and the deleter retries, sees
the hold, and backs off. Zero responsive documents are lost -- and the
transaction log proves it.

Invariant protected: a HELD document is NEVER deleted.
Usage: py -3.11 src/hold_firewall.py [num_docs] [num_deleters]
"""
import os, sys, time
from concurrent.futures import ThreadPoolExecutor
import psycopg
from dotenv import load_dotenv

load_dotenv(r"B:\ColdCase\.env")
URL = os.environ["CRDB_ADMIN_URL"]
DOCS = int(sys.argv[1]) if len(sys.argv) > 1 else 200
DELETERS = int(sys.argv[2]) if len(sys.argv) > 2 else 4
GAP = 0.002  # the check->delete window a deleter leaves open

def setup():
    c = psycopg.connect(URL, autocommit=True)
    c.execute("""CREATE TABLE IF NOT EXISTS hold_docs(
        doc_id INT PRIMARY KEY, responsive BOOL, held BOOL, deleted BOOL)""")
    c.execute("DELETE FROM hold_docs")
    with c.cursor() as cur:
        cur.executemany("INSERT INTO hold_docs VALUES(%s,true,false,false)",
                        [(i,) for i in range(DOCS)])
    c.close()

def reset():
    c = psycopg.connect(URL, autocommit=True)
    c.execute("UPDATE hold_docs SET held=false, deleted=false")
    c.close()

def spoliation():
    """Held documents that were nonetheless deleted -- destroyed evidence."""
    c = psycopg.connect(URL, autocommit=True)
    n = c.execute("SELECT count(*) FROM hold_docs WHERE held AND deleted").fetchone()[0]
    c.close()
    return n

# ---- the litigation-hold agent: place a hold on every responsive doc ----
def hold_agent_naive():
    c = psycopg.connect(URL, autocommit=True)
    for i in range(DOCS):
        c.execute("UPDATE hold_docs SET held=true WHERE doc_id=%s AND responsive", (i,))
    c.close()

# ---- a retention/cron deleter: purge docs that are NOT on hold ----
def deleter_naive(worker):
    c = psycopg.connect(URL, autocommit=True)
    for i in range(DOCS):
        held = c.execute("SELECT held FROM hold_docs WHERE doc_id=%s", (i,)).fetchone()[0]
        if not held:                       # check
            time.sleep(GAP)                # ... hold can land in this window ...
            c.execute("UPDATE hold_docs SET deleted=true WHERE doc_id=%s", (i,))  # delete
    c.close()

# ---- same actors, but check-and-delete is SERIALIZABLE and retries ----
def hold_agent_serializable():
    c = psycopg.connect(URL, autocommit=False)
    for i in range(DOCS):
        while True:
            try:
                with c.transaction():
                    c.execute("UPDATE hold_docs SET held=true WHERE doc_id=%s AND responsive", (i,))
                break
            except psycopg.errors.SerializationFailure:
                continue
    c.close()

def deleter_serializable(worker):
    c = psycopg.connect(URL, autocommit=False)
    for i in range(DOCS):
        while True:
            try:
                with c.transaction():
                    held = c.execute("SELECT held FROM hold_docs WHERE doc_id=%s", (i,)).fetchone()[0]
                    if not held:
                        time.sleep(GAP)
                        c.execute("UPDATE hold_docs SET deleted=true WHERE doc_id=%s", (i,))
                break
            except psycopg.errors.SerializationFailure:
                continue        # a hold landed concurrently -> retry, re-read, back off
    c.close()

def run(label, hold_fn, del_fn):
    reset()
    t = time.time()
    with ThreadPoolExecutor(max_workers=DELETERS + 1) as ex:
        futs = [ex.submit(hold_fn)]
        futs += [ex.submit(del_fn, w) for w in range(DELETERS)]
        for f in futs: f.result()
    lost = spoliation()
    print(f"{label:<32} evidence_destroyed={lost:<4} ({time.time()-t:.1f}s)")
    return lost


if __name__ == "__main__":
    setup()
    print(f"{DELETERS} retention/cron deleters purge non-held docs while the hold")
    print(f"agent places a litigation hold on {DOCS} responsive documents, at once:\n")
    naive = run("Naive check-then-delete:", hold_agent_naive, deleter_naive)
    ser   = run("CockroachDB SERIALIZABLE:", hold_agent_serializable, deleter_serializable)
    print()
    if naive > 0 and ser == 0:
        print(f"Naive isolation DESTROYED {naive} held documents -- spoliation.")
        print("SERIALIZABLE lost ZERO: the hold is a bulletproof, legally")
        print("defensible snapshot. Every deleter that raced a hold was")
        print("detected and forced to back off. That is the CockroachDB")
        print("difference -- and the transaction log is the audit trail.")
    else:
        print(f"naive spoliation={naive}, serializable spoliation={ser}")
    import json
    json.dump({"docs": DOCS, "deleters": DELETERS, "naive": naive, "serializable": ser},
              open(r"B:\HoldFirewall\docs\spoliation_result.json", "w"))
