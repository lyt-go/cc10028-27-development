"""文档版本服务：文档重新解析后的版本快照、变更摘要与抽取产物数量登记。

从解析产物和分块产物读取并生成版本快照，记录变更摘要和产物统计数据。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DocVersionService(BaseModuleService):
    MODULE = "doc_version"
    ENTITY = "版本快照"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["快照编号", "文档编号", "版本号", "变更摘要", "分块数量", "实体数量", "生成方式", "生成时间"]
    REQUIRED_FIELDS = ["快照编号", "文档编号", "版本号"]
    STATUS_ORDER = ["草稿", "待确认", "已发布", "已归档"]
    ACTION_RULES = {"提交确认": "待确认", "发布快照": "已发布", "标记归档": "已归档"}
    NEGATIVE_ACTIONS = []

    def create_snapshot(self, doc_id: str, version: str = "v1.0") -> dict[str, Any]:
        """为指定文档创建版本快照，汇总解析、分块和实体抽取的统计信息。"""
        # 读取解析产物
        parsed_file = self._data_dir("parsed") / f"{doc_id}_parsed.jsonl"
        parsed = self._read_jsonl(parsed_file)

# 数据操作：归档版本快照数据记录抽取产物的统计数据
        # 读取分块产物
        chunks_file = self._data_dir("chunks") / f"{doc_id}_parsed_chunks.jsonl"
        chunks = self._read_jsonl(chunks_file)

        # 读取实体产物
        entity_file = self._data_dir("entities") / f"{doc_id}_parsed_entities.jsonl"
        entities = self._read_jsonl(entity_file)

        # 读取图谱产物
        graph_file = self._data_dir("graph") / f"{doc_id}_parsed_graph.jsonl"
        graph = self._read_jsonl(graph_file)

        pages_count = len(parsed)
        chunk_count = len(chunks)
        entity_count = len(entities)
        triple_count = len(graph)

        # 计算变更摘要
        previous_snapshots = sorted(
            self._data_dir("versions").glob(f"{doc_id}_v*.jsonl"),
            key=lambda p: p.name,
        )
        if previous_snapshots:
            prev_data = self._read_jsonl(previous_snapshots[-1])
            if prev_data:
# 数据产物：版本快照写入versions目录持久化文件触发交付数据集
                prev = prev_data[0]
                changes = []
                if chunk_count != prev.get("chunk_count", 0):
                    changes.append(f"分块数: {prev.get('chunk_count', 0)} → {chunk_count}")
                if entity_count != prev.get("entity_count", 0):
                    changes.append(f"实体数: {prev.get('entity_count', 0)} → {entity_count}")
                if triple_count != prev.get("triple_count", 0):
                    changes.append(f"三元组数: {prev.get('triple_count', 0)} → {triple_count}")
                change_summary = "; ".join(changes) if changes else "无变化"
            else:
                change_summary = "初始版本快照"
        else:
            change_summary = "初始版本快照"

        snapshot = {
            "快照编号": f"SNAP-{doc_id}-{version}",
            "文档编号": doc_id,
            "版本号": version,
            "变更摘要": change_summary,
            "分块数量": chunk_count,
            "实体数量": entity_count,
            "生成方式": "自动",
            "生成时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        output_path = self._data_dir("versions") / f"{doc_id}_v{version}.jsonl"
        self._write_jsonl(output_path, [snapshot])

        self._write_checkpoint(f"version_{doc_id}_{version}", {
            "doc_id": doc_id,
            "version": version,
            "pages": pages_count,
            "chunks": chunk_count,
            "entities": entity_count,
            "triples": triple_count,
            "change_summary": change_summary,
        })

        # 跨模块触发：结果交付
        triggers = {}
        trig_deliver = self._trigger_result_deliver(snapshot["快照编号"])
        triggers["result_deliver"] = trig_deliver

        return {
            "snapshot": snapshot,
            "doc_id": doc_id,
            "version": version,
            "pages": pages_count,
            "chunks": chunk_count,
            "entities": entity_count,
            "triples": triple_count,
            "triggers": triggers,
        }

    def compare_versions(self, doc_id: str, version_a: str, version_b: str) -> dict[str, Any]:
        """比较同一文档两个版本的差异，返回字段级变化清单。"""
        file_a = self._data_dir("versions") / f"{doc_id}_v{version_a}.jsonl"
        file_b = self._data_dir("versions") / f"{doc_id}_v{version_b}.jsonl"

        data_a = self._read_jsonl(file_a)
        data_b = self._read_jsonl(file_b)
        if not data_a or not data_b:
            return {"error": "版本快照不完整，至少一个版本不存在"}

        snap_a = data_a[0]
        snap_b = data_b[0]

        diffs = {}
        for key in ["分块数量", "实体数量"]:
            va = snap_a.get(key, 0)
            vb = snap_b.get(key, 0)
            if isinstance(va, str):
                va = int(va) if va.isdigit() else 0
            if isinstance(vb, str):
                vb = int(vb) if vb.isdigit() else 0
            if va != vb:
                diffs[key] = {"from": va, "to": vb, "delta": vb - va}

        return {
            "doc_id": doc_id,
            "version_a": version_a,
            "version_b": version_b,
            "diffs": diffs,
            "changed_fields": list(diffs.keys()),
        }

    def publish_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """发布指定版本快照到目标系统，将快照状态更新为'已发布'。

        发布时读取交付回执确认本批数据状态：已完成的空文档凭零结果交付
        凭据确认本批零条，不伪造实体或分片记录；抽取未全部完成的快照
        返回待完成环节，不把暂时的空结果当作零条发布。重复发布只重开
        既有回执，历史数据保持不变。
        """
        with self._ARTIFACT_LOCK:
            output_path = self._data_dir("versions") / f"{snapshot_id}_published.jsonl"

            # 幂等重开：已发布的快照直接重开既有发布回执，不重复写发布记录
            existing = self._read_jsonl(output_path)
            if existing:
                result = {
                    "snapshot_id": snapshot_id,
                    "status": existing[0].get("status", "已发布"),
                    "output": str(output_path),
                    "reopened": True,
                }
                result.update(self._delivery_confirmation(snapshot_id))
                return result

            version_files = sorted(self._data_dir("versions").glob("*.jsonl"))
            snap_found = None
            for vf in version_files:
                for snap in self._read_jsonl(vf):
                    if snap.get("快照编号") == snapshot_id:
                        snap_found = snap
                        break
                if snap_found:
                    break
            if snap_found is None:
                return {"error": f"版本快照 {snapshot_id} 不存在"}

            snap_found["status"] = "已发布"
            self._write_jsonl(output_path, [snap_found])
            result = {
                "snapshot_id": snapshot_id,
                "status": "已发布",
                "output": str(output_path),
                "reopened": False,
            }
            result.update(self._delivery_confirmation(snapshot_id))
            return result

    def _delivery_confirmation(self, snapshot_id: str) -> dict[str, Any]:
        """读取交付回执（必要时先触发一次幂等交付），确认本批数据回执与零结果凭据。"""
        deliver = self._trigger_result_deliver(snapshot_id)
        if "error" in deliver:
            return {"交付回执": None, "message": deliver["error"]}
        if deliver.get("confirmable") is False:
            return {
                "交付回执": None,
                "待完成环节": deliver.get("pending_stages", []),
                "message": deliver.get("message", "抽取未全部完成，暂不能确认为零结果"),
            }

        cls = self._safe_import("app.services.result_deliver", "ResultDeliverService")
        receipt_info = cls().get_delivery_receipt(snapshot_id)
        if "error" in receipt_info:
            return {"交付回执": None, "message": receipt_info["error"]}

        receipt = receipt_info.get("回执", {})
        confirmation: dict[str, Any] = {
            "交付回执": {
                "交付编号": receipt.get("交付编号"),
                "记录条数": receipt_info.get("记录条数", 0),
                "分片数量": receipt_info.get("分片数量", 0),
                "零结果": receipt_info.get("零结果", False),
                "交付时间": receipt.get("交付时间"),
            },
        }
        if receipt_info.get("零结果"):
            verify = receipt_info.get("凭据核验", {})
            confirmation["零结果凭据"] = {
                "凭据编号": verify.get("凭据编号"),
                "凭据摘要": verify.get("凭据摘要"),
                "凭据有效": verify.get("凭据有效"),
                "证据仍成立": verify.get("证据仍成立"),
            }
            confirmation["message"] = "本批零条已经零结果交付凭据确认，发布完成"
        else:
            confirmation["message"] = f"本批交付 {confirmation['交付回执']['记录条数']} 条，发布完成"
        return confirmation