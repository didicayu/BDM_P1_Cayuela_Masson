from __future__ import annotations

import ast
from pathlib import Path
import unittest


class OrchestrationContractTests(unittest.TestCase):
    def test_end_to_end_dag_triggers_every_required_stage_in_order(self) -> None:
        path = Path("orchestration/airflow/dags/end_to_end_pipeline_dag.py")
        source = path.read_text(encoding="utf-8")
        ast.parse(source)

        expected_dags = (
            "cybersecintel_api_expansion_ingestion",
            "cybersecintel_warm_stream_aggregates",
            "cybersecintel_trusted_zone",
            "cybersecintel_exploitation_zone",
            "cybersecintel_consumption_exports",
        )
        for dag_id in expected_dags:
            self.assertIn(dag_id, source)

        expected_chain = (
            "trigger_api_ingestion >> trigger_warm_aggregates >> trigger_trusted_zone "
            ">> trigger_exploitation_zone >> trigger_consumption"
        )
        self.assertIn(expected_chain, source)


if __name__ == "__main__":
    unittest.main()
