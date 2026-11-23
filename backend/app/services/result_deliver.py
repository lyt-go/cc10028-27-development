"""结果交付服务：抽取结果按交付格式向目标系统的分片导出与交付记录登记。

从版本快照读取产物，按指定格式和分片策略导出到目标系统。
对已完成抽取但结果为零条的快照，出具可核对的零结果交付凭据；
抽取未全部完成（中断或部分失败）的快照不得把暂时的空结果确认为零条。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ResultDeliverService(BaseModuleService):
    MODULE = "result_deliver"
    ENTITY = "交付任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["交付编号", "快照编号", "交付格式", "目标系统", "字段范围", "记录条数", "分片大小", "交付时间"]
    REQUIRED_FIELDS = ["交付编号", "快照编号", "交付格式"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    ACTION_RULES = {"启动导出": "待导出", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    SUPPORTED_FORMATS = {
        "jsonl": {"mime": "application/x-ndjson", "description": "每行一条JSON记录的文本格式"},
        "csv": {"mime": "text/csv", "description": "逗号分隔值表格格式"},
        "parquet": {"mime": "application/octet-stream", "description": "列式存储格式"},
        "xlsx": {"mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "description": "Excel表格格式"},
    }

    # 零结果确认所需的抽取环节检查点（按管线顺序，{doc_id} 为文档编号占位）。
    # 任一环节缺检查点都说明抽取未全部完成，暂时的空结果不能确认为零条。
    ZERO_RESULT_STAGES = [
        ("格式解析", "parse_{doc_id}"),
        ("分块切分", "chunk_{doc_id}_parsed"),
        ("实体识别", "entity_{doc_id}_parsed"),
        ("向量嵌入", "vector_{doc_id}_parsed"),
    ]
    # 关系抽取仅在实体识别产出实体时才要求完成；零实体时关系必然为零，免抽取。
    RELATION_STAGE = ("关系抽取", "relation_{doc_id}_parsed")

    # 各环节检查点中参与凭据摘要计算的稳定统计键（不含时间戳等易变字段），
    # 保证同一抽取结果重开回执时凭据摘要可重复核对。
    STAGE_DIGEST_KEYS = {
        "格式解析": ["pages", "records_written"],
        "分块切分": ["chunks_count"],
        "实体识别": ["chunks_processed", "entities_found"],
        "向量嵌入": ["chunks_embedded"],
        "关系抽取": ["entities_found", "relations_extracted"],
    }
# 数据操作：导出交付任务数据将版本快照产物分发到目标系统

    def deliver_from_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """从版本快照导出抽取产物到目标系统，支持分片分批交付。

        同一快照重复请求只重开既有交付回执，不重复写分片，保证旧包内容不变。
        产物为零条时先核对抽取完成证据：全部完成才出具零结果交付凭据，
        否则返回待完成环节，不把中断或部分失败解释成零条。
        """
        with self._ARTIFACT_LOCK:
            # 幂等重开：已交付的快照直接返回既有回执，历史交付包保持原样
            existing = self._read_jsonl(self._delivery_file(snapshot_id))
            if existing:
                return self._reopen_result(snapshot_id, existing[0])

            snapshot_data = self._find_snapshot(snapshot_id)
            if not snapshot_data:
                return {"error": f"版本快照 {snapshot_id} 不存在，请先创建版本快照"}

            doc_id = snapshot_data.get("文档编号", "")
            fmt = "jsonl"
            target = "下游知识库"

            # 收集所有可交付的抽取产物
            deliverables = self._collect_deliverables(doc_id)

            if not deliverables:
                return self._deliver_zero_result(snapshot_id, doc_id, fmt, target)

            shard_size = 500
            shards = []
# 数据产物：交付分片文件写入deliveries目录持久化文件
            batch = []
            for item in deliverables:
                batch.append(item)
                if len(batch) >= shard_size:
                    shards.append(batch)
                    batch = []
            if batch:
                shards.append(batch)

            # 写入交付产物文件
            output_files = []
            for i, shard in enumerate(shards):
                output_path = self._data_dir("deliveries") / f"{snapshot_id}_shard_{i+1:03d}.jsonl"
                self._write_jsonl(output_path, shard)
                output_files.append(str(output_path))

            delivery = {
                "交付编号": f"DELIVER-{snapshot_id}",
                "快照编号": snapshot_id,
                "交付格式": fmt,
                "目标系统": target,
                "字段范围": f"实体+关系+三元组+向量",
                "记录条数": len(deliverables),
                "分片大小": shard_size,
                "分片数量": len(shards),
                "零结果": False,
                "交付时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            self._write_jsonl(self._delivery_file(snapshot_id), [delivery])

            self._write_checkpoint(f"deliver_{snapshot_id}", {
                "snapshot_id": snapshot_id,
                "format": fmt,
                "target": target,
                "total_records": len(deliverables),
                "shards": len(shards),
                "zero_result": False,
            })

            return {
                "snapshot_id": snapshot_id,
                "delivery_id": delivery["交付编号"],
                "format": fmt,
                "target": target,
                "total_records": len(deliverables),
                "shards": len(shards),
                "zero_result": False,
                "output_files": output_files,
                "reopened": False,
            }

    # -- 零结果交付：完成证据核对与凭据出具 --

    def assess_extraction_completion(self, doc_id: str) -> dict[str, Any]:
        """核对指定文档各抽取环节的完成证据，判定零条结果是否可确认。"""
        evidence = []
        pending = []
        for stage_name, cp_tmpl in self.ZERO_RESULT_STAGES:
            cp_name = cp_tmpl.format(doc_id=doc_id)
            cp = self._read_checkpoint(cp_name)
            if cp is None:
                pending.append(stage_name)
                continue
            evidence.append(self._stage_evidence(stage_name, cp_name, cp))

        # 关系抽取：实体为零时必然零关系（免抽取）；有实体时必须有关系检查点，
        # 否则属于部分失败，不能把缺失的关系产物解释成零条。
        entity_cp = self._read_checkpoint(f"entity_{doc_id}_parsed")
        entities_found = int(entity_cp.get("entities_found", 0) or 0) if entity_cp else 0
        rel_stage, rel_tmpl = self.RELATION_STAGE
        rel_name = rel_tmpl.format(doc_id=doc_id)
        rel_cp = self._read_checkpoint(rel_name)
        if rel_cp is not None:
            evidence.append(self._stage_evidence(rel_stage, rel_name, rel_cp))
        elif entities_found > 0:
            pending.append(rel_stage)
        else:
            evidence.append({
                "环节": rel_stage,
                "检查点": rel_name,
                "统计": {"entities_found": 0, "relations_extracted": 0},
                "完成方式": "零实体免抽取",
            })

        return {
            "doc_id": doc_id,
            "complete": not pending,
            "evidence": evidence,
            "pending_stages": pending,
        }

    def _deliver_zero_result(self, snapshot_id: str, doc_id: str, fmt: str, target: str) -> dict[str, Any]:
        """产物为零条时核对完成证据，全部完成才出具零结果交付凭据。"""
        completion = self.assess_extraction_completion(doc_id)
        if not completion["complete"]:
            pending = "、".join(completion["pending_stages"])
            return {
                "snapshot_id": snapshot_id,
                "doc_id": doc_id,
                "zero_result": False,
                "confirmable": False,
                "total_records": 0,
                "pending_stages": completion["pending_stages"],
                "message": f"抽取环节未全部完成（待完成：{pending}），当前零条只是暂时状态，不能确认为零结果",
                "confirmable_when": f"待{pending}完成后重新请求交付，方可确认本批是否零条",
            }

        credential = self._build_zero_credential(snapshot_id, doc_id, completion["evidence"])
        delivery = {
            "交付编号": f"DELIVER-{snapshot_id}",
            "快照编号": snapshot_id,
            "交付格式": fmt,
            "目标系统": target,
            "字段范围": "实体+关系+三元组+向量",
            "记录条数": 0,
            "分片大小": 500,
            "分片数量": 0,
            "零结果": True,
            "凭据编号": credential["凭据编号"],
            "零结果凭据": credential["凭据摘要"],
            "交付时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        # 零结果交付只写回执与凭据，不写任何分片文件，不伪造实体或分片记录
        self._write_jsonl(self._delivery_file(snapshot_id), [delivery])
        self._write_jsonl(self._zero_credential_file(snapshot_id), [credential])

        self._write_checkpoint(f"deliver_{snapshot_id}", {
            "snapshot_id": snapshot_id,
            "format": fmt,
            "target": target,
            "total_records": 0,
            "shards": 0,
            "zero_result": True,
            "credential": credential["凭据摘要"],
        })

        return {
            "snapshot_id": snapshot_id,
            "delivery_id": delivery["交付编号"],
            "format": fmt,
            "target": target,
            "total_records": 0,
            "shards": 0,
            "zero_result": True,
            "confirmable": True,
            "credential_id": credential["凭据编号"],
            "credential": credential["凭据摘要"],
            "evidence": completion["evidence"],
            "output_files": [],
            "reopened": False,
        }

    # -- 回执重开与凭据核对（只读，不写任何文件） --

    def get_delivery_receipt(self, snapshot_id: str) -> dict[str, Any]:
        """重新打开交付回执：读取既有交付记录，零结果回执附带凭据核对结果。"""
        records = self._read_jsonl(self._delivery_file(snapshot_id))
        if not records:
            return {"error": f"快照 {snapshot_id} 尚无交付回执，请先执行交付"}
        receipt = records[0]
        result = {
            "snapshot_id": snapshot_id,
            "回执": receipt,
            "记录条数": int(receipt.get("记录条数", 0) or 0),
            "分片数量": int(receipt.get("分片数量", 0) or 0),
            "零结果": bool(receipt.get("零结果")),
        }
        if receipt.get("零结果"):
            result["凭据核验"] = self.verify_zero_credential(snapshot_id)
        return result

    def verify_zero_credential(self, snapshot_id: str) -> dict[str, Any]:
        """核对零结果交付凭据：重算凭据摘要，并复查完成证据当前仍然成立。"""
        creds = self._read_jsonl(self._zero_credential_file(snapshot_id))
        if not creds:
            return {"error": f"快照 {snapshot_id} 无零结果交付凭据"}
        cred = creds[0]
        evidence = cred.get("完成证据", [])
        recomputed = self._evidence_digest(cred.get("快照编号", ""), cred.get("文档编号", ""), evidence)
        digest_ok = recomputed == cred.get("凭据摘要")

        # 复查当前检查点：若抽取被中断或重跑导致统计变化，旧凭据的证据不再成立
        current = self.assess_extraction_completion(cred.get("文档编号", ""))
        still_holds = (
            current["complete"]
            and self._evidence_digest_parts(current["evidence"]) == self._evidence_digest_parts(evidence)
        )

        return {
            "凭据编号": cred.get("凭据编号"),
            "快照编号": cred.get("快照编号"),
            "凭据摘要": cred.get("凭据摘要"),
            "凭据有效": digest_ok,
            "证据仍成立": still_holds,
            "确认时间": cred.get("确认时间"),
        }

    # -- 内部工具 --

    def _delivery_file(self, snapshot_id: str):
        return self._data_dir("deliveries") / f"{snapshot_id}_delivery.jsonl"

    def _zero_credential_file(self, snapshot_id: str):
        return self._data_dir("deliveries") / f"{snapshot_id}_zero_credential.jsonl"

    def _find_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        """在版本快照产物中查找指定快照编号。"""
        version_files = sorted(self._data_dir("versions").glob("*.jsonl"))
        for vf in version_files:
            for snap in self._read_jsonl(vf):
                if snap.get("快照编号") == snapshot_id:
                    return snap
        return None

    def _reopen_result(self, snapshot_id: str, receipt: dict[str, Any]) -> dict[str, Any]:
        """把既有交付回执转换成交付结果形状，标记为重开，不触碰任何文件。"""
        return {
            "snapshot_id": snapshot_id,
            "delivery_id": receipt.get("交付编号"),
            "format": receipt.get("交付格式"),
            "target": receipt.get("目标系统"),
            "total_records": int(receipt.get("记录条数", 0) or 0),
            "shards": int(receipt.get("分片数量", 0) or 0),
            "zero_result": bool(receipt.get("零结果")),
            "credential_id": receipt.get("凭据编号"),
            "credential": receipt.get("零结果凭据"),
            "reopened": True,
        }

    def _stage_evidence(self, stage: str, cp_name: str, cp: dict[str, Any]) -> dict[str, Any]:
        """从检查点提取环节完成证据，统计字段只取稳定键以保证摘要可重算。"""
        keys = self.STAGE_DIGEST_KEYS.get(stage, [])
        stats = {k: cp.get(k, 0) for k in keys}
        return {
            "环节": stage,
            "检查点": cp_name,
            "统计": stats,
            "完成时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cp.get("_updated_at", 0))),
        }

    @staticmethod
    def _evidence_digest_parts(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """提取证据中参与摘要计算的稳定部分（环节、检查点名、统计）。"""
        return [
            {"环节": e.get("环节"), "检查点": e.get("检查点"), "统计": e.get("统计", {})}
            for e in evidence
        ]

    def _evidence_digest(self, snapshot_id: str, doc_id: str, evidence: list[dict[str, Any]]) -> str:
        """对快照编号、文档编号与完成证据的稳定部分计算凭据摘要。"""
        canonical = json.dumps({
            "快照编号": snapshot_id,
            "文档编号": doc_id,
            "完成证据": self._evidence_digest_parts(evidence),
        }, ensure_ascii=False, sort_keys=True)
        return self._compute_checksum(canonical)

    def _build_zero_credential(self, snapshot_id: str, doc_id: str, evidence: list[dict[str, Any]]) -> dict[str, Any]:
        """出具零结果交付凭据：零条结论加全部抽取环节的完成证据与可核对摘要。"""
        return {
            "凭据编号": f"ZERO-{snapshot_id}",
            "快照编号": snapshot_id,
            "文档编号": doc_id,
            "记录条数": 0,
            "分片数量": 0,
            "完成证据": evidence,
            "凭据摘要": self._evidence_digest(snapshot_id, doc_id, evidence),
            "确认时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _collect_deliverables(self, doc_id: str) -> list[dict[str, Any]]:
        """收集指定文档的所有可交付抽取产物。"""
        items = []

        # 收集实体
        entity_files = sorted(self._data_dir("entities").glob(f"{doc_id}*.jsonl"))
        for ef in entity_files:
            for e in self._read_jsonl(ef):
                items.append({"type": "entity", "source": str(ef), "data": e})

        # 收集关系
        relation_files = sorted(self._data_dir("relations").glob(f"{doc_id}*.jsonl"))
        for rf in relation_files:
            for r in self._read_jsonl(rf):
                items.append({"type": "relation", "source": str(rf), "data": r})

        # 收集向量
        vector_files = sorted(self._data_dir("vectors").glob(f"{doc_id}*.jsonl"))
        for vf in vector_files:
            for v in self._read_jsonl(vf):
                items.append({"type": "vector", "source": str(vf), "data": {
                    "chunk_id": v.get("chunk_id"),
                    "doc_id": v.get("doc_id"),
                    "dimension": v.get("vector_dim"),
                    "index": v.get("index"),
                }})

        return items

    def list_formats(self) -> list[dict[str, Any]]:
        """列出所有支持的交付格式。"""
        return [
            {"format": k, "mime": v["mime"], "description": v["description"]}
            for k, v in self.SUPPORTED_FORMATS.items()
        ]
