```python
# tests/test_chunks.py
from src.preprocessing import split_documents
import pandas as pd

def test_split_documents_basic():
    df = pd.DataFrame([{"doc_id":"A1","content":"one two three four five six"}])
    out = split_documents(df, chunk_size=3, overlap=1)
    assert len(out) >= 2
    assert set(out.columns) == {"doc_id","chunk_id","chunk_text"}
```
