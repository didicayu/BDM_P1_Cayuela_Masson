from __future__ import annotations

import unittest

from ingestion.common.delta_storage import DeltaLakeStorage


class DeltaStorageTests(unittest.TestCase):
    def test_keyed_merge_batches_keep_the_last_duplicate_source_row(self) -> None:
        rows = DeltaLakeStorage._deduplicate_merge_records(
            [
                {"id": "1", "value": "old"},
                {"id": "2", "value": "only"},
                {"id": "1", "value": "new"},
            ],
            ["id"],
        )

        self.assertEqual(rows, [{"id": "1", "value": "new"}, {"id": "2", "value": "only"}])

    def test_append_batches_are_not_deduplicated(self) -> None:
        rows = [{"id": "1"}, {"id": "1"}]

        self.assertIs(DeltaLakeStorage._deduplicate_merge_records(rows, None), rows)


if __name__ == "__main__":
    unittest.main()
