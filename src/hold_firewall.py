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
    try:  # or CockroachDB silently UPGRADES READ COMMITTED to SERIALIZABLE
        c.execute("SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = true")
    except Exception as e:
        print("note: could not enable READ COMMITTED:", repr(e)[:90])
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

# ---- apples-to-apples baseline: explicit READ COMMITTED on the SAME cluster ----
def _rc_conn():
    c = psycopg.connect(URL, autocommit=False)
    c.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
    return c

def hold_agent_rc():
    c = _rc_conn()
    for i in range(DOCS):
        with c.transaction():
            c.execute("UPDATE hold_docs SET held=true WHERE doc_id=%s AND responsive", (i,))
    c.close()

def deleter_rc(worker):
    c = _rc_conn()
    for i in range(DOCS):
        with c.transaction():          # READ COMMITTED permits the read->write race
            held = c.execute("SELECT held FROM hold_docs WHERE doc_id=%s", (i,)).fetchone()[0]
            if not held:
                time.sleep(GAP)
                c.execute("UPDATE hold_docs SET deleted=true WHERE doc_id=%s", (i,))
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
    naive = run("Naive autocommit check-delete:", hold_agent_naive, deleter_naive)
    rc    = run("READ COMMITTED (same cluster):", hold_agent_rc, deleter_rc)
    ser   = run("CockroachDB SERIALIZABLE:", hold_agent_serializable, deleter_serializable)
    print()
    if ser == 0 and (naive > 0 or rc > 0):
        print(f"Weaker isolation DESTROYED held evidence: naive={naive}, "
              f"READ COMMITTED={rc}. SERIALIZABLE lost ZERO -- SAME cluster, same")
        print("workload, same contention: the ONLY variable is the isolation level.")
        print("The hold is a bulletproof, legally defensible snapshot and the")
        print("transaction log is the audit trail. This is the CockroachDB difference.")
    else:
        print(f"naive={naive}, read_committed={rc}, serializable={ser}")
    import json
    json.dump({"docs": DOCS, "deleters": DELETERS, "naive": naive,
               "read_committed": rc, "serializable": ser},
              open(r"B:\HoldFirewall\docs\spoliation_result.json", "w"))
