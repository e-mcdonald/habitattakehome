"""Extract stage — pulls records from a source and lands them as JSONL."""

from habitat_pipeline.extract.runner import ExtractResult, run_extract

__all__ = ["ExtractResult", "run_extract"]
