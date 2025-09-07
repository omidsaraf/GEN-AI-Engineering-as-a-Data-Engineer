
```python
import pandas as pd, re

def clean_text(t: str) -> str:
    t = re.sub(r"\s+", " ", t or "").replace("\u200b", "").strip()
    return t

def split_documents(df: pd.DataFrame, chunk_size: int = 512, overlap: int = 50):
    chunks = []
    for _, r in df.iterrows():
        words = (r["content"] or "").split()
        for i in range(0, len(words), chunk_size - overlap):
            chunks.append({"doc_id": r["doc_id"],
                           "chunk_id": f"{r['doc_id']}_{i}",
                           "chunk_text": clean_text(" ".join(words[i:i+chunk_size]))})
    return pd.DataFrame(chunks)
```
