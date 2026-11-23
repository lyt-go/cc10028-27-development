"""零结果交付凭据验收测试：空结果的零条校验、交付确认与发布回执。

覆盖场景：
- 已完成抽取的空文档出具可核对的零结果交付凭据；
- 仍在执行（向量未完成）的文档不得确认为零结果；
- 中断或部分失败（实体有产出但关系缺失）不得解释成零条；
- 回执可重复打开且凭据摘要不变，不重复写分片；
- 提交发布与旧包下载并发时历史数据照常读取且内容不变。
"""
import os
import sys
import json as _json
import uuid as _uuid
import threading

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from app.services.format_parse import FormatParseService
from app.services.chunk_split import ChunkSplitService
from app.services.entity_recognize import EntityRecognizeService
from app.services.relation_extract import RelationExtractService
from app.services.vector_embed import VectorEmbedService
from app.services.doc_version import DocVersionService
from app.services.result_deliver import ResultDeliverService
from app.services.base import DATA_ROOT


def _uid(prefix="ID"):
    return f"{prefix}-{_uuid.uuid4().hex[:8].upper()}"


def _make_completed_empty_doc(doc_id):
    """构造一个抽取已全部完成、结果为零条的文档（只写完成证据检查点）。"""
    svc = ResultDeliverService()
    svc._write_checkpoint(f"parse_{doc_id}", {
        "doc_id": doc_id, "pages": 1, "records_written": 1,
    })
    svc._write_checkpoint(f"chunk_{doc_id}_parsed", {
        "parsed_doc_id": f"{doc_id}_parsed", "chunks_count": 0,
    })
    svc._write_checkpoint(f"entity_{doc_id}_parsed", {
        "chunk_set_id": f"{doc_id}_parsed", "chunks_processed": 0, "entities_found": 0,
    })
    svc._write_checkpoint(f"vector_{doc_id}_parsed", {
        "chunk_set_id": f"{doc_id}_parsed", "chunks_embedded": 0,
    })
    return doc_id


def _make_running_doc(doc_id):
    """构造一个仍在执行的文档：解析/分块/实体已完成，向量嵌入仍未完成。"""
    svc = ResultDeliverService()
    svc._write_checkpoint(f"parse_{doc_id}", {
        "doc_id": doc_id, "pages": 1, "records_written": 1,
    })
    svc._write_checkpoint(f"chunk_{doc_id}_parsed", {
        "parsed_doc_id": f"{doc_id}_parsed", "chunks_count": 2,
    })
    svc._write_checkpoint(f"entity_{doc_id}_parsed", {
        "chunk_set_id": f"{doc_id}_parsed", "chunks_processed": 2, "entities_found": 0,
    })
    # 故意不写 vector 检查点：向量仍未完成
    return doc_id


def _snapshot_of(doc_id, version="v1.0"):
    result = DocVersionService().create_snapshot(doc_id, version)
    assert "error" not in result, result.get("error", "")
    return result


class TestZeroResultCredential:
    """已完成空文档的零条校验与零结果交付凭据"""

    def test_completed_empty_doc_issues_credential(self):
        doc_id = _make_completed_empty_doc(_uid("DOC"))
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]

        # create_snapshot 触发的交付应走零结果路径
        trig = snap["triggers"]["result_deliver"]
        assert trig["zero_result"] is True
        assert trig["total_records"] == 0
        assert trig["shards"] == 0
        assert trig["credential"]

        # 凭据文件已出具，且可核对
        cred_file = DATA_ROOT / "deliveries" / f"{snapshot_id}_zero_credential.jsonl"
        assert cred_file.exists()
        verify = ResultDeliverService().verify_zero_credential(snapshot_id)
        assert verify["凭据有效"] is True
        assert verify["证据仍成立"] is True
        assert verify["凭据摘要"] == trig["credential"]

    def test_zero_result_writes_no_shards_and_no_fake_entities(self):
        doc_id = _make_completed_empty_doc(_uid("DOC"))
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]

        # 零结果交付不得伪造分片记录
        shards = list((DATA_ROOT / "deliveries").glob(f"{snapshot_id}_shard_*.jsonl"))
        assert shards == []
        # 不得伪造实体产物
        assert list((DATA_ROOT / "entities").glob(f"{doc_id}*.jsonl")) == []

        # 交付回执登记为零条、零分片
        receipt = ResultDeliverService().get_delivery_receipt(snapshot_id)
        assert receipt["零结果"] is True
        assert receipt["记录条数"] == 0
        assert receipt["分片数量"] == 0
        assert receipt["凭据核验"]["凭据有效"] is True

    def test_publish_confirms_zero_receipt(self):
        doc_id = _make_completed_empty_doc(_uid("DOC"))
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]

        pub = DocVersionService().publish_snapshot(snapshot_id)
        assert pub["status"] == "已发布"
        assert pub["交付回执"]["零结果"] is True
        assert pub["交付回执"]["记录条数"] == 0
        assert pub["零结果凭据"]["凭据有效"] is True
        assert pub["零结果凭据"]["证据仍成立"] is True
        assert "零条" in pub["message"]


class TestRunningDocNotZero:
    """仍在执行的文档：暂时的空结果不得确认为零条"""

    def test_vector_pending_not_confirmable(self):
        doc_id = _make_running_doc(_uid("DOC"))
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]

        trig = snap["triggers"]["result_deliver"]
        assert trig["zero_result"] is False
        assert trig["confirmable"] is False
        assert "向量嵌入" in trig["pending_stages"]
        # 接收方需要知道何时能确认：返回中须说明确认前提
        assert "向量嵌入" in trig["confirmable_when"]

        # 不出具凭据、不写交付回执
        assert not (DATA_ROOT / "deliveries" / f"{snapshot_id}_zero_credential.jsonl").exists()
        assert not (DATA_ROOT / "deliveries" / f"{snapshot_id}_delivery.jsonl").exists()

    def test_publish_reports_pending_stages(self):
        doc_id = _make_running_doc(_uid("DOC"))
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]

        pub = DocVersionService().publish_snapshot(snapshot_id)
        assert pub["status"] == "已发布"
        assert pub["交付回执"] is None
        assert "向量嵌入" in pub["待完成环节"]
        assert "零结果凭据" not in pub

    def test_partial_failure_not_interpreted_as_zero(self):
        """实体有产出但关系抽取缺失（部分失败），不得解释成零条。"""
        doc_id = _uid("DOC")
        svc = ResultDeliverService()
        svc._write_checkpoint(f"parse_{doc_id}", {"doc_id": doc_id, "pages": 1, "records_written": 1})
        svc._write_checkpoint(f"chunk_{doc_id}_parsed", {"parsed_doc_id": f"{doc_id}_parsed", "chunks_count": 2})
        svc._write_checkpoint(f"entity_{doc_id}_parsed", {
            "chunk_set_id": f"{doc_id}_parsed", "chunks_processed": 2, "entities_found": 3,
        })
        svc._write_checkpoint(f"vector_{doc_id}_parsed", {
            "chunk_set_id": f"{doc_id}_parsed", "chunks_embedded": 2,
        })
        # 实体文件缺失（产物丢失）且关系检查点缺失：属于部分失败
        snap = _snapshot_of(doc_id)
        trig = snap["triggers"]["result_deliver"]
        assert trig["confirmable"] is False
        assert "关系抽取" in trig["pending_stages"]
        assert not (DATA_ROOT / "deliveries" / f"{snap['snapshot']['快照编号']}_zero_credential.jsonl").exists()

    def test_interrupted_doc_not_interpreted_as_zero(self):
        """只有解析检查点（中断），不得解释成零条。"""
        doc_id = _uid("DOC")
        ResultDeliverService()._write_checkpoint(f"parse_{doc_id}", {
            "doc_id": doc_id, "pages": 1, "records_written": 1,
        })
        snap = _snapshot_of(doc_id)
        trig = snap["triggers"]["result_deliver"]
        assert trig["confirmable"] is False
        assert set(trig["pending_stages"]) == {"分块切分", "实体识别", "向量嵌入"}


class TestReceiptReopen:
    """回执重开：幂等、凭据摘要不变、不重复写文件"""

    def test_reopen_delivery_is_idempotent(self):
        doc_id = _make_completed_empty_doc(_uid("DOC"))
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]
        svc = ResultDeliverService()

        first = svc.deliver_from_snapshot(snapshot_id)
        assert first["reopened"] is True  # create_snapshot 已触发过交付
        cred_before = (DATA_ROOT / "deliveries" / f"{snapshot_id}_zero_credential.jsonl").read_bytes()
        delivery_before = (DATA_ROOT / "deliveries" / f"{snapshot_id}_delivery.jsonl").read_bytes()

        again = svc.deliver_from_snapshot(snapshot_id)
        assert again["reopened"] is True
        assert again["credential"] == first["credential"]
        assert again["total_records"] == 0
        # 文件内容不变、不追加重复记录
        assert (DATA_ROOT / "deliveries" / f"{snapshot_id}_zero_credential.jsonl").read_bytes() == cred_before
        assert (DATA_ROOT / "deliveries" / f"{snapshot_id}_delivery.jsonl").read_bytes() == delivery_before
        assert len(svc._read_jsonl(DATA_ROOT / "deliveries" / f"{snapshot_id}_delivery.jsonl")) == 1

    def test_reopen_receipt_verifies_credential(self):
        doc_id = _make_completed_empty_doc(_uid("DOC"))
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]
        svc = ResultDeliverService()

        r1 = svc.get_delivery_receipt(snapshot_id)
        r2 = svc.get_delivery_receipt(snapshot_id)
        assert r1["凭据核验"]["凭据摘要"] == r2["凭据核验"]["凭据摘要"]
        assert r1["凭据核验"]["凭据有效"] is True
        assert r1["凭据核验"]["证据仍成立"] is True

    def test_republish_reopens_without_duplicates(self):
        doc_id = _make_completed_empty_doc(_uid("DOC"))
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]
        version_svc = DocVersionService()

        p1 = version_svc.publish_snapshot(snapshot_id)
        assert p1["reopened"] is False
        p2 = version_svc.publish_snapshot(snapshot_id)
        assert p2["reopened"] is True
        assert p2["零结果凭据"]["凭据摘要"] == p1["零结果凭据"]["凭据摘要"]
        published = version_svc._read_jsonl(DATA_ROOT / "versions" / f"{snapshot_id}_published.jsonl")
        assert len(published) == 1


class TestOldPackageIntegrity:
    """旧版本交付包：新版本发布期间旧数据照常读取且不变化"""

    def _deliver_normal_doc(self):
        """走完整管线产出一个有实体的普通交付包。"""
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
        VectorEmbedService().embed_from_chunk(f"{doc_id}_parsed")
        snap = _snapshot_of(doc_id)
        snapshot_id = snap["snapshot"]["快照编号"]
        result = ResultDeliverService().deliver_from_snapshot(snapshot_id)
        assert result["total_records"] > 0
        return snapshot_id

    def test_old_package_unchanged_after_zero_publish(self):
        old_snap = self._deliver_normal_doc()
        deliveries = DATA_ROOT / "deliveries"
        old_files = sorted(deliveries.glob(f"{old_snap}_*.jsonl"))
        assert old_files, "旧交付包应包含分片与回执文件"
        old_bytes = {f.name: f.read_bytes() for f in old_files}

        # 同一交付窗口：一个已完成空文档、一个仍在执行的文档分别发布
        empty_snap = _snapshot_of(_make_completed_empty_doc(_uid("DOC")))["snapshot"]["快照编号"]
        running_snap = _snapshot_of(_make_running_doc(_uid("DOC")))["snapshot"]["快照编号"]
        DocVersionService().publish_snapshot(empty_snap)
        DocVersionService().publish_snapshot(running_snap)

        # 重新打开旧包回执，旧数据未变化
        for name, data in old_bytes.items():
            assert (deliveries / name).read_bytes() == data, f"旧交付包 {name} 被改动"
        receipt = ResultDeliverService().get_delivery_receipt(old_snap)
        assert receipt["零结果"] is False
        assert receipt["记录条数"] > 0

    def test_concurrent_publish_and_old_package_download(self):
        old_snap = self._deliver_normal_doc()
        deliveries = DATA_ROOT / "deliveries"
        old_bytes = {f.name: f.read_bytes() for f in deliveries.glob(f"{old_snap}_*.jsonl")}

        empty_snap = _snapshot_of(_make_completed_empty_doc(_uid("DOC")))["snapshot"]["快照编号"]
        errors = []
        stop = threading.Event()

        def downloader():
            # 旧版本还有人在下载：并发读取旧交付包与回执
            while not stop.is_set():
                for name, data in old_bytes.items():
                    try:
                        if (deliveries / name).read_bytes() != data:
                            errors.append(f"旧包 {name} 内容变化")
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"读取旧包失败: {e}")
                receipt = ResultDeliverService().get_delivery_receipt(old_snap)
                if "error" in receipt:
                    errors.append(receipt["error"])

        def publisher():
            for _ in range(5):
                DocVersionService().publish_snapshot(empty_snap)

        readers = [threading.Thread(target=downloader) for _ in range(3)]
        for t in readers:
            t.start()
        pub = threading.Thread(target=publisher)
        pub.start()
        pub.join()
        stop.set()
        for t in readers:
            t.join()

        assert not errors, errors[:3]
        # 并发结束后旧包内容仍未变化
        for name, data in old_bytes.items():
            assert (deliveries / name).read_bytes() == data
