"""Replay with PYTHONPATH pointing to Range@1275b0c/tools/phase2."""
import gzip
import hashlib
import json
from pathlib import Path

from judge import delivery

base = Path(__file__).resolve().parent
manifest = json.loads((base / "manifest.json").read_text())
records = {}
for name, expected in manifest.items():
    compressed = (base / (name + ".gz")).read_bytes()
    assert hashlib.sha256(compressed).hexdigest() == expected["compressed_sha256"], name
    data = gzip.decompress(compressed)
    assert len(data) == expected["bytes"], name
    assert hashlib.sha256(data).hexdigest() == expected["sha256"], name
    records[name] = [json.loads(line) for line in data.splitlines()]
result = delivery(records["sent.jsonl"], records["consumer.jsonl"],
                  records["status.jsonl"], records["faults.jsonl"])
assert result == json.loads((base / "verdict.json").read_text()), result
assert result["pass"], result
print(json.dumps(result, indent=2))
