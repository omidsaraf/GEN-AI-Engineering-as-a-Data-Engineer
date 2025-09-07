```python
from langgraph.graph import StateGraph
from typing import Dict

class St(Dict): ...

g = StateGraph(St)

def tool_search(state):
    # call retriever; add results into state["context"]
    return state

def tool_answer(state):
    # call LLM with prompt
    return state

g.add_node("search", tool_search)

g.add_node("answer", tool_answer)

g.add_edge("search", "answer")
app = g.compile()
```
