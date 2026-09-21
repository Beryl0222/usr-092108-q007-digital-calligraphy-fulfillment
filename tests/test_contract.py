import json
import unittest
from pathlib import Path

from src.fulfillment.envelope import AGGREGATE_TYPES, EVENT_TYPES
from src.validator import validate_event

ROOT = Path(__file__).parents[1]


class ContractTest(unittest.TestCase):
    def test_sample_matches_envelope(self) -> None:
        sample = json.loads((ROOT / "data" / "sample.json").read_text(encoding="utf-8"))
        self.assertEqual(validate_event(sample), [])

    def test_code_enums_match_schema(self) -> None:
        schema = json.loads((ROOT / "contracts" / "domain.schema.json").read_text(encoding="utf-8"))
        self.assertEqual(set(EVENT_TYPES), set(schema["properties"]["event_type"]["enum"]))
        self.assertEqual(set(AGGREGATE_TYPES), set(schema["properties"]["aggregate_type"]["enum"]))


if __name__ == "__main__":
    unittest.main()
