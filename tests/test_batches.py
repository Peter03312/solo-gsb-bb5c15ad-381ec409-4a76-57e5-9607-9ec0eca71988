"""端到端自测：同一交接车次多份样本批量移交。

运行：python -m tests.test_batches
"""
import os
import tempfile
import threading

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "test_batches.db")
os.environ["SEED_ON_START"] = "false"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import init_db  # noqa: E402
from app.main import app  # noqa: E402

init_db()
client = TestClient(app)
client.__enter__()  # 触发 lifespan（启动钩子）
API = "/api/v1"
PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def reg(code, collector="张三", seal=None, when="2026-09-30T01:00:00Z"):
    return client.post(f"{API}/samples", json={
        "sample_code": code, "collected_at": when,
        "collector": collector, "seal_no": seal or f"SEAL-{code}"
    })


def ev(payload):
    return client.post(f"{API}/events", json=payload)


def chain(code):
    return client.get(f"{API}/samples/{code}/chain").json()


def batch(body):
    return client.post(f"{API}/transfer-batches", json=body)


def get_batch(bid):
    return client.get(f"{API}/transfer-batches/{bid}")


def correction(body):
    return client.post(f"{API}/corrections", json=body)


def item_by_code(view, code):
    return next(i for i in view["items"] if i["sample_code"] == code)


# ---------------------------------------------------------------- 1. 正常批量提交，逐份即既有 transfer 事件
print("1) 一批三份样本整体提交，逐份生成移交并即时重算")
for code in ("G1", "G2", "G3"):
    reg(code)

bat = {
    "batch_id": "BAT-GOOD", "transferred_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四",
    "items": [
        {"sample_code": "G1", "transfer_no": "TR-G1", "event_id": "GE-1"},
        {"sample_code": "G2", "transfer_no": "TR-G2", "event_id": "GE-2"},
        {"sample_code": "G3", "transfer_no": "TR-G3", "event_id": "GE-3"},
    ],
}
r = batch(bat)
check("批量提交 201", r.status_code == 201, r.text)
b = r.json()
check("批次头回显（统一时间/交出人/接收人/提交时间）",
      b["batch"]["batch_id"] == "BAT-GOOD" and b["batch"]["transferred_at"] == "2026-09-30T02:00:00Z"
      and b["batch"]["from_custodian"] == "张三" and b["batch"]["to_custodian"] == "李四"
      and b["batch"]["submitted_at"], str(b["batch"]))
check("total=3 且顺序与提交一致",
      b["total"] == 3 and [i["sample_code"] for i in b["items"]] == ["G1", "G2", "G3"], str(b["items"]))
for code, no, eid in (("G1", "TR-G1", "GE-1"), ("G2", "TR-G2", "GE-2"), ("G3", "TR-G3", "GE-3")):
    it = item_by_code(b, code)
    check(f"{code} 逐份结论 pending（已移交缺签收缺入库）",
          it["evaluation"]["status"] == "pending"
          and any("缺少签收" in x for x in it["evaluation"]["reasons"]), str(it["evaluation"]["reasons"]))
    check(f"{code} 原事件仍有效、单号/事件号正确",
          it["event_effective"] is True and it["superseded_by"] is None
          and it["transfer_no"] == no and it["event_id"] == eid, str(it))
    c = chain(code)
    check(f"{code} 链上恰有 1 条 transfer 且带 batch_id 原始归属",
          len(c["timeline"]) == 1 and c["timeline"][0]["event_type"] == "transfer"
          and c["timeline"][0]["event_id"] == eid and c["timeline"][0]["batch_id"] == "BAT-GOOD",
          str(c["timeline"]))
    check(f"{code} 待签收期间保管人仍为交出人张三", c["current_custodian"] == "张三")

r = get_batch("BAT-GOOD")
check("批次查询 200 且内容一致",
      r.status_code == 200 and r.json()["total"] == 3
      and [i["event_id"] for i in r.json()["items"]] == ["GE-1", "GE-2", "GE-3"], r.text)

# 批量移交事件就是普通 transfer：补齐签收→检查→入库后 valid，原事件始终有效
ev({"event_id": "GE-1-R", "event_type": "receive", "sample_code": "G1",
    "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "TR-G1",
    "receiver": "李四", "seal_no": "SEAL-G1"})
ev({"event_id": "GE-1-C", "event_type": "seal_check", "sample_code": "G1",
    "occurred_at": "2026-09-30T03:30:00Z", "checked_seal_no": "SEAL-G1", "seal_intact": True})
ev({"event_id": "GE-1-S", "event_type": "storage", "sample_code": "G1",
    "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})
check("补齐后续链后 valid", chain("G1")["status"] == "valid", str(chain("G1")["reasons"]))
v = get_batch("BAT-GOOD").json()
check("批次查询反映 G1 最新结论 valid 且原移交仍有效",
      item_by_code(v, "G1")["evaluation"]["status"] == "valid"
      and item_by_code(v, "G1")["event_effective"] is True, str(v))

# 交出人不是当前保管人 → 与单事件同样判 invalid（批量事件参与统一核验）
reg("G4", collector="王五")
r = batch({"batch_id": "BAT-BADFROM", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "G4", "transfer_no": "TR-G4", "event_id": "GE-4"}]})
check("非当前保管人批量移交：提交成功但核验 invalid（普通 transfer 语义）",
      r.status_code == 201
      and item_by_code(r.json(), "G4")["evaluation"]["status"] == "invalid", r.text)

# ---------------------------------------------------------------- 2. 整批失败不留痕
print("2) 任一不满足条件 → 整批失败，无批次、无事件残留")

def fresh_pair(prefix):
    reg(prefix + "-A")
    reg(prefix + "-B")


# 2.1 批内样本重复
fresh_pair("DUP-S")
r = batch({"batch_id": "BAT-DUPS", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "DUP-S-A", "transfer_no": "TR-1", "event_id": "DS-1"},
                     {"sample_code": "DUP-S-A", "transfer_no": "TR-2", "event_id": "DS-2"}]})
check("批内样本重复 409 batch_duplicate_sample",
      r.status_code == 409 and r.json()["error"]["code"] == "batch_duplicate_sample", r.text)
check("失败批次不可查", get_batch("BAT-DUPS").status_code == 404)
check("失败未写入任何事件", len(chain("DUP-S-A")["timeline"]) == 0, str(chain("DUP-S-A")["timeline"]))

# 2.2 批内事件号重复
fresh_pair("DUP-E")
r = batch({"batch_id": "BAT-DUPE", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "DUP-E-A", "transfer_no": "TR-1", "event_id": "SAME-EV"},
                     {"sample_code": "DUP-E-B", "transfer_no": "TR-2", "event_id": "SAME-EV"}]})
check("批内事件号重复 409 batch_duplicate_event",
      r.status_code == 409 and r.json()["error"]["code"] == "batch_duplicate_event", r.text)
check("失败批次不可查", get_batch("BAT-DUPE").status_code == 404)
check("两份样本均无事件", all(len(chain(c)["timeline"]) == 0 for c in ("DUP-E-A", "DUP-E-B")))

# 2.3 样本不存在：即便排在第二份，整批失败
fresh_pair("MISS")
r = batch({"batch_id": "BAT-MISS", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "MISS-A", "transfer_no": "TR-1", "event_id": "MS-1"},
                     {"sample_code": "GHOST", "transfer_no": "TR-2", "event_id": "MS-2"}]})
check("样本不存在 404 sample_not_found",
      r.status_code == 404 and r.json()["error"]["code"] == "sample_not_found", r.text)
check("失败批次不可查", get_batch("BAT-MISS").status_code == 404)
check("第一份样本也未写入事件", len(chain("MISS-A")["timeline"]) == 0)

# 2.4 事件号已被既有事件占用（含单事件接口此前写入）
fresh_pair("OCC")
ev({"event_id": "OCC-EV", "event_type": "transfer", "sample_code": "OCC-A",
    "occurred_at": "2026-09-30T01:30:00Z", "transfer_no": "TR-OLD",
    "from_custodian": "张三", "to_custodian": "某甲"})
r = batch({"batch_id": "BAT-OCC", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "OCC-B", "transfer_no": "TR-1", "event_id": "OCC-NEW"},
                     {"sample_code": "OCC-A", "transfer_no": "TR-2", "event_id": "OCC-EV"}]})
check("事件号被占用 409 event_id_occupied",
      r.status_code == 409 and r.json()["error"]["code"] == "event_id_occupied", r.text)
check("失败批次不可查", get_batch("BAT-OCC").status_code == 404)
check("未占用的样本也未连带写入",
      len(chain("OCC-B")["timeline"]) == 0 and len(chain("OCC-A")["timeline"]) == 1,
      str((chain("OCC-B")["timeline"], chain("OCC-A")["timeline"])))

# 2.5 样本已核定：整批 409 history_locked，不留痕
reg("LOCK-B", collector="张三", seal="SEAL-LOCK-B")
client.post(f"{API}/samples/G1/approve", json={"approver": "质量主管"})  # G1 valid → 核定成功
check("前置：G1 已核定", chain("G1")["approved_storage_at"] is not None)
r = batch({"batch_id": "BAT-LOCK", "transferred_at": "2026-09-30T06:00:00Z",
           "from_custodian": "实验室", "to_custodian": "李四",
           "items": [{"sample_code": "LOCK-B", "transfer_no": "TR-1", "event_id": "LB-1"},
                     {"sample_code": "G1", "transfer_no": "TR-2", "event_id": "LB-2"}]})
check("含已核定样本 409 history_locked",
      r.status_code == 409 and r.json()["error"]["code"] == "history_locked", r.text)
check("失败批次不可查", get_batch("BAT-LOCK").status_code == 404)
check("未核定样本未被连带写入", len(chain("LOCK-B")["timeline"]) == 0)

# 校验失败（空 items / 多余字段 / 缺批次头）→ 422，且不占批次号
r = client.post(f"{API}/transfer-batches", json={
    "batch_id": "BAT-EMPTY", "transferred_at": "2026-09-30T02:00:00Z",
    "from_custodian": "张三", "to_custodian": "李四", "items": []})
check("空 items 422", r.status_code == 422, r.text)
fresh_pair("VAL")
r = batch({"batch_id": "BAT-EXTRA", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四", "vehicle": "车次X",
           "items": [{"sample_code": "VAL-A", "transfer_no": "TR-1", "event_id": "VA-1"}]})
check("批次头多余字段 422", r.status_code == 422, r.text)
r = batch({"batch_id": "BAT-ITEMX", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "VAL-A", "transfer_no": "TR-1",
                      "event_id": "VA-1", "note": "item 不允许备注"}]})
check("item 多余字段 422", r.status_code == 422, r.text)
r = batch({"transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "VAL-A", "transfer_no": "TR-1", "event_id": "VA-1"}]})
check("缺批次号 422", r.status_code == 422, r.text)
# 被 422 拒绝的批次号与事件号随后可正常使用（无占用）
r = batch({"batch_id": "BAT-EXTRA", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "VAL-A", "transfer_no": "TR-1", "event_id": "VA-1"}]})
check("校验失败不占批次号，随后可提交 201", r.status_code == 201, r.text)

# ---------------------------------------------------------------- 3. 幂等与冲突
print("3) 同批次号同内容幂等（核定后亦然）、异内容冲突")
same = batch(bat)
check("同批次号同内容重试 200", same.status_code == 200, same.text)
check("幂等响应无重复写入（仍 3 份）",
      same.json()["total"] == 3
      and len(chain("G2")["timeline"]) == 1, str(same.json()["total"]))

diff_time = {**bat, "transferred_at": "2026-09-30T02:30:00Z"}
r = batch(diff_time)
check("改统一移交时间 409 batch_conflict",
      r.status_code == 409 and r.json()["error"]["code"] == "batch_conflict", r.text)
diff_to = {**bat, "to_custodian": "赵六"}
check("改接收人 409 batch_conflict", batch(diff_to).status_code == 409)
diff_from = {**bat, "from_custodian": "另一人"}
check("改交出人 409 batch_conflict", batch(diff_from).status_code == 409)
diff_item = {**bat, "items": [{**bat["items"][0], "transfer_no": "TR-CHANGED"},
                              *bat["items"][1:]]}
check("改某份移交单号 409 batch_conflict", batch(diff_item).status_code == 409)
diff_ev = {**bat, "items": [{**bat["items"][0], "event_id": "GE-1-X"},
                            *bat["items"][1:]]}
check("改某份事件号 409 batch_conflict", batch(diff_ev).status_code == 409)
check("冲突后批次内容不变",
      [i["transfer_no"] for i in get_batch("BAT-GOOD").json()["items"]]
      == ["TR-G1", "TR-G2", "TR-G3"])

# G1 已核定（见 2.5）：同批次号同内容仍 200；异内容仍 409（幂等/冲突优先于冻结）
check("样本已核定后同批次号同内容重试仍 200", batch(bat).status_code == 200)
check("样本已核定后异内容仍是 409 batch_conflict（而非 history_locked）",
      batch(diff_to).status_code == 409)

check("未知批次查询 404 batch_not_found",
      get_batch("NO-SUCH-BATCH").status_code == 404
      and client.get(f"{API}/transfer-batches/NO-SUCH-BATCH").json()["error"]["code"] == "batch_not_found")

# ---------------------------------------------------------------- 4. 与单事件接口互通
print("4) 批量事件与既有单事件接口共用同一指纹/唯一号")
# 批量已写入 GE-2（G2）：单事件接口同号同内容 → 200；异内容 → 409
single_same = ev({"event_id": "GE-2", "event_type": "transfer", "sample_code": "G2",
                  "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-G2",
                  "from_custodian": "张三", "to_custodian": "李四"})
check("单事件接口对批量事件同内容重试 200",
      single_same.status_code == 200 and single_same.json()["idempotent"] is True, single_same.text)
single_diff = ev({"event_id": "GE-2", "event_type": "transfer", "sample_code": "G2",
                  "occurred_at": "2026-09-30T02:10:00Z", "transfer_no": "TR-G2",
                  "from_custodian": "张三", "to_custodian": "李四"})
check("单事件接口对批量事件异内容 409 event_conflict（行为不变）",
      single_diff.status_code == 409
      and single_diff.json()["error"]["code"] == "event_conflict", single_diff.text)

# 反向：单事件新占号 → 后续批量引用失败（已在 2.4 覆盖），此处确认核定后单事件语义不变
r = ev({"event_id": "GE-1", "event_type": "transfer", "sample_code": "G1",
        "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-G1",
        "from_custodian": "张三", "to_custodian": "李四"})
check("核定后单事件同内容仍 200 幂等", r.status_code == 200, r.text)
r = ev({"event_id": "GE-1", "event_type": "transfer", "sample_code": "G1",
        "occurred_at": "2026-09-30T02:01:00Z", "transfer_no": "TR-G1",
        "from_custodian": "张三", "to_custodian": "李四"})
check("核定后单事件异内容仍 409", r.status_code == 409, r.text)

# ---------------------------------------------------------------- 5. 纠错只改有效链与查询，批次原始归属可追溯
print("5) 作废/替换批量原事件：批次查询随之变化，原始归属仍可追溯")
# G3 仍只有批量移交；作废它
r = correction({"correction_id": "FIX-G3", "sample_code": "G3",
                "target_event_id": "GE-3", "action": "void",
                "operator": "质控员", "reason": "批量移交作废演示"})
check("批量事件可作废 201（纠错接口行为不变）", r.status_code == 201, r.text)
v = get_batch("BAT-GOOD").json()
it3 = item_by_code(v, "G3")
check("作废后批次查询：原事件 event_effective=false 且 superseded_by 指向纠错号",
      it3["event_effective"] is False and it3["superseded_by"] == "FIX-G3", str(it3))
check("作废后该份结论回到无有效移交（缺入库，pending）",
      it3["evaluation"]["status"] == "pending" and it3["evaluation"]["event_count"] == 0,
      str(it3["evaluation"]))
c = chain("G3")
check("chain 有效时间线已不含 GE-3，superseded_events 留痕且保留 batch_id 归属",
      len(c["timeline"]) == 0
      and [e["event_id"] for e in c["superseded_events"]] == ["GE-3"]
      and c["superseded_events"][0]["batch_id"] == "BAT-GOOD", str((c["timeline"], c["superseded_events"])))
check("批次头不被纠错改变",
      v["batch"]["batch_id"] == "BAT-GOOD" and v["batch"]["to_custodian"] == "李四", str(v["batch"]))
# 同批其他份不受影响
check("同批 G2 原事件仍有效", item_by_code(v, "G2")["event_effective"] is True, str(v))

# 替换：G2 的批量移交被替换为新事件号
r = correction({"correction_id": "FIX-G2", "sample_code": "G2",
                "target_event_id": "GE-2", "action": "replace",
                "operator": "质控员", "reason": "批量移交替换演示",
                "new_event": {"event_id": "GE-2-FIX", "event_type": "transfer",
                              "occurred_at": "2026-09-30T02:00:00Z",
                              "transfer_no": "TR-G2",
                              "from_custodian": "张三", "to_custodian": "李四"}})
check("批量事件可替换 201", r.status_code == 201, r.text)
v = get_batch("BAT-GOOD").json()
check("替换后批次项仍指向原事件号且标记失效",
      item_by_code(v, "G2")["event_id"] == "GE-2"
      and item_by_code(v, "G2")["event_effective"] is False, str(v))
c = chain("G2")
check("有效链为新号（replaces_event_id 指原号），新事件不继承批次归属（batch_id=null）",
      [e["event_id"] for e in c["timeline"]] == ["GE-2-FIX"]
      and c["timeline"][0]["replaces_event_id"] == "GE-2"
      and c["timeline"][0]["batch_id"] is None
      and [e["event_id"] for e in c["superseded_events"]] == ["GE-2"], str(c))

# 核定后拒绝纠错的既有行为：G1 已核定
r = correction({"correction_id": "FIX-G1-LATE", "sample_code": "G1",
                "target_event_id": "GE-1", "action": "void",
                "operator": "质控员", "reason": "核定后试探"})
check("核定后对批量事件纠错仍 409 history_locked",
      r.status_code == 409 and r.json()["error"]["code"] == "history_locked", r.text)

# ---------------------------------------------------------------- 6. 并发
print("6) 并发：按已提交顺序判定（批次/批次、批次/核定）")

# 6.1 同批次号同内容并发 → 一次新建 + 一次幂等，无重复
for code in ("CC1", "CC2"):
    reg(code)
same_body = {"batch_id": "BAT-CC-SAME", "transferred_at": "2026-09-30T02:00:00Z",
             "from_custodian": "张三", "to_custodian": "李四",
             "items": [{"sample_code": "CC1", "transfer_no": "TR-1", "event_id": "CC-E1"},
                       {"sample_code": "CC2", "transfer_no": "TR-2", "event_id": "CC-E2"}]}
statuses_same: dict[str, int] = {}
barrier = threading.Barrier(2)


def post_batch(tag, body, sink):
    barrier.wait()
    sink[tag] = batch(body).status_code


t1 = threading.Thread(target=post_batch, args=("a", same_body, statuses_same))
t2 = threading.Thread(target=post_batch, args=("b", same_body, statuses_same))
t1.start(); t2.start(); t1.join(); t2.join()
check("同内容并发：恰一次 201，另一次 200 幂等",
      sorted(statuses_same.values()) == [200, 201], str(statuses_same))
check("无重复事件",
      all(len(chain(c)["timeline"]) == 1 for c in ("CC1", "CC2")), str(statuses_same))

# 6.2 同批次号异内容并发 → 一个成功，另一个 409
for code in ("CX1",):
    reg(code)
body_x = {"batch_id": "BAT-CC-X", "transferred_at": "2026-09-30T02:00:00Z",
          "from_custodian": "张三", "to_custodian": "李四",
          "items": [{"sample_code": "CX1", "transfer_no": "TR-X", "event_id": "CX-E"}]}
body_y = {**body_x, "to_custodian": "赵六"}
statuses_diff: dict[str, int] = {}
barrier2 = threading.Barrier(2)
t1 = threading.Thread(target=post_batch, args=("x", body_x, statuses_diff))
t2 = threading.Thread(target=post_batch, args=("y", body_y, statuses_diff))
t1.start(); t2.start(); t1.join(); t2.join()
check("异内容并发：一个 201，另一个 409 batch_conflict",
      sorted(statuses_diff.values()) == [201, 409], str(statuses_diff))

# 6.3 两个不同批次并发抢占同一事件号 → 一个成功，另一个整批 409 且无残留
reg("SHARE-A"); reg("SHARE-B"); reg("SHARE-C")
body_p = {"batch_id": "BAT-SHARE-P", "transferred_at": "2026-09-30T02:00:00Z",
          "from_custodian": "张三", "to_custodian": "李四",
          "items": [{"sample_code": "SHARE-A", "transfer_no": "TR-P1", "event_id": "SHARED-EV"},
                    {"sample_code": "SHARE-B", "transfer_no": "TR-P2", "event_id": "P-ONLY"}]}
body_q = {"batch_id": "BAT-SHARE-Q", "transferred_at": "2026-09-30T02:00:00Z",
          "from_custodian": "张三", "to_custodian": "李四",
          "items": [{"sample_code": "SHARE-A", "transfer_no": "TR-Q1", "event_id": "SHARED-EV"},
                    {"sample_code": "SHARE-C", "transfer_no": "TR-Q2", "event_id": "Q-ONLY"}]}
statuses_share: dict[str, int] = {}
barrier3 = threading.Barrier(2)
t1 = threading.Thread(target=post_batch, args=("p", body_p, statuses_share))
t2 = threading.Thread(target=post_batch, args=("q", body_q, statuses_share))
t1.start(); t2.start(); t1.join(); t2.join()
check("抢同事件号并发：一个 201，另一个 409 event_id_occupied",
      sorted(statuses_share.values()) == [201, 409], str(statuses_share))
if statuses_share["p"] == 201:
    winner, loser, loser_only_sample = "BAT-SHARE-P", "BAT-SHARE-Q", "SHARE-C"
else:
    winner, loser, loser_only_sample = "BAT-SHARE-Q", "BAT-SHARE-P", "SHARE-B"
check("胜方批次可查", get_batch(winner).status_code == 200)
check("败方整批无残留（批次不存在，独有样本无事件）",
      get_batch(loser).status_code == 404 and len(chain(loser_only_sample)["timeline"]) == 0,
      str((get_batch(loser).status_code, chain(loser_only_sample)["timeline"])))
check("共用样本只有胜方一份移交", len(chain("SHARE-A")["timeline"]) == 1,
      str(chain("SHARE-A")["timeline"]))

# 6.4 批量提交与核定并发：按已提交顺序判定
reg("RACE-X", collector="张三", seal="SEAL-RX")
ev({"event_id": "RX-T", "event_type": "transfer", "sample_code": "RACE-X",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-RX-T",
    "from_custodian": "张三", "to_custodian": "李四"})
ev({"event_id": "RX-R", "event_type": "receive", "sample_code": "RACE-X",
    "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "TR-RX-T",
    "receiver": "李四", "seal_no": "SEAL-RX"})
ev({"event_id": "RX-C", "event_type": "seal_check", "sample_code": "RACE-X",
    "occurred_at": "2026-09-30T03:30:00Z", "checked_seal_no": "SEAL-RX", "seal_intact": True})
ev({"event_id": "RX-S", "event_type": "storage", "sample_code": "RACE-X",
    "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})
check("前置：RACE-X 当前 valid（可核定）", chain("RACE-X")["status"] == "valid")
race_out: dict[str, str] = {}
barrier4 = threading.Barrier(2)
race_batch_body = {"batch_id": "BAT-RACE", "transferred_at": "2026-09-30T05:00:00Z",
                   "from_custodian": "实验室", "to_custodian": "李四",
                   "items": [{"sample_code": "RACE-X", "transfer_no": "TR-RX-NEW",
                              "event_id": "RX-BAT"}]}


def race_batch_worker():
    barrier4.wait()
    race_out["batch"] = str(batch(race_batch_body).status_code)


def race_approve_worker():
    barrier4.wait()
    race_out["approve"] = str(client.post(f"{API}/samples/RACE-X/approve",
                                          json={"approver": "质量主管"}).status_code)


t1 = threading.Thread(target=race_batch_worker)
t2 = threading.Thread(target=race_approve_worker)
t1.start(); t2.start(); t1.join(); t2.join()
# 核定先提交 → 核定 200 冻结 + 批量 409 history_locked；
# 批量先提交 → 批量 201（新增在途移交，链转 pending）+ 核定基于最终链 422。
if race_out.get("approve") == "200":
    check("核定先提交：200 冻结", True)
    check("批量被 409 history_locked 拒绝", race_out.get("batch") == "409", str(race_out))
    check("失败批量无残留", get_batch("BAT-RACE").status_code == 404)
else:
    check("批量先提交：201", race_out.get("batch") == "201", str(race_out))
    check("核定基于最终有效链拒绝 422", race_out.get("approve") == "422", str(race_out))
    c = chain("RACE-X")
    check("新移交已在链上且样本未核定",
          any(e["event_id"] == "RX-BAT" for e in c["timeline"])
          and c["approved_storage_at"] is None, str(c))

# ---------------------------------------------------------------- 7. 失败不占号、错误信封格式
print("7) 业务失败后批次号/事件号不被占用")
reg("RETRY-X"); reg("RETRY-Y")
fail_body = {"batch_id": "BAT-RETRY", "transferred_at": "2026-09-30T02:00:00Z",
             "from_custodian": "张三", "to_custodian": "李四",
             "items": [{"sample_code": "RETRY-X", "transfer_no": "TR-1", "event_id": "RT-1"},
                       {"sample_code": "RETRY-Y", "transfer_no": "TR-2", "event_id": "OCC-EV"}]}
r = batch(fail_body)
check("引用占用号先失败 409", r.status_code == 409 and r.json()["error"]["code"] == "event_id_occupied", r.text)
ok_body = {"batch_id": "BAT-RETRY", "transferred_at": "2026-09-30T02:00:00Z",
           "from_custodian": "张三", "to_custodian": "李四",
           "items": [{"sample_code": "RETRY-X", "transfer_no": "TR-1", "event_id": "RT-1"},
                     {"sample_code": "RETRY-Y", "transfer_no": "TR-2", "event_id": "RT-2"}]}
r = batch(ok_body)
check("同批次号更换未占用事件号后可正常提交 201", r.status_code == 201, r.text)
r = get_batch("GHOST")
check("404 错误信封 code/message",
      r.status_code == 404 and r.json()["error"]["code"] == "batch_not_found"
      and r.json()["error"]["message"], r.text)

print(f"\n结果: {PASS} 通过, {FAIL} 失败")
raise SystemExit(1 if FAIL else 0)
