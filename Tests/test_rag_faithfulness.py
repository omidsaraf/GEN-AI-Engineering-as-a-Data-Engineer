```python
from typing import List
from src.rag import make_qa

# stub LLM & retriever would be injected in real tests

def faithfulness_score(answer: str, context: List[str]) -> float:
    # heuristic: penalize unsupported claims
    ctx = " ".join(context).lower()
    unsupported = sum(1 for sent in answer.split('.') if sent and sent.lower() not in ctx)
    total = max(1, len(answer.split('.')))
    return max(0.0, 1.0 - unsupported/total)

def test_faithfulness_basic():
    ans = "A is part of B. C is unrelated."
    ctx = ["A is part of B"]
    s = faithfulness_score(ans, ctx)
    assert 0.0 <= s <= 1.0
```
