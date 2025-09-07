
```python
from langchain.vectorstores import FAISS
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate

def make_qa(retriever, llm):
    prompt = PromptTemplate(
      input_variables=["context","question"],
      template=("Use ONLY the context to answer.\nContext:\n{context}\nQ: {question}\nA:")
    )
    return RetrievalQA.from_chain_type(llm=llm, retriever=retriever, chain_type="stuff", chain_type_kwargs={"prompt": prompt})
```
