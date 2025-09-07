```python
# tests/test_rag_eval.py
from src.rag import build_qa
import numpy as np

def precision_at_k(retrieved, relevant, k=5):
    return len(set(retrieved[:k]) & set(relevant)) / max(k,1)

def test_eval_sample():
    # pretend ids
    retrieved = ["d1","d2","d3","d4","d5"]
    relevant = ["d2","d9","d5"]
    p5 = precision_at_k(retrieved, relevant, 5)
    assert 0.0 <= p5 <= 1.0
```
