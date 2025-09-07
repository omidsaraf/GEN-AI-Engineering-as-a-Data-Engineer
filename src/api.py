```python
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="NILOOMID AI API")

class Q(BaseModel):
    question: str

@app.post("/qa")
def qa(q: Q):
    # 1) retrieve via FAISS  2) LLM answer  3) return
    return {"answer": "stub"}
```
