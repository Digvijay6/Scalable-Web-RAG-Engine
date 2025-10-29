import uuid
import pydantic
import google.generativeai as genai
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session

from worker import process_url, embedding_model, collection
from database import SessionLocal, engine
from models import Base, IngestionJob, JobStatus
from config import GOOGLE_API_KEY

# Configure the Gemini client
genai.configure(api_key=GOOGLE_API_KEY)
llm = genai.GenerativeModel('gemini-2.5-pro')

# Create the database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Scalable Web-Aware RAG Engine",
    description="An API to ingest web content and query it using a RAG pipeline.",
)


# Pydantic models for request/response validation
class URLIngestRequest(pydantic.BaseModel):
    url: pydantic.HttpUrl


class IngestResponse(pydantic.BaseModel):
    job_id: uuid.UUID
    status: JobStatus


class QueryRequest(pydantic.BaseModel):
    query: str


class QueryResponse(pydantic.BaseModel):
    answer: str
    source_urls: list[str]


# Dependency to get a DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/ingest-url", status_code=202, response_model=IngestResponse)
def ingest_url(request: URLIngestRequest, db: Session = Depends(get_db)):
    """
    Accepts a URL for ingestion, creates a background job,
    and returns a job ID for status tracking.
    """
    new_job = IngestionJob(url=str(request.url))
    db.add(new_job)
    db.commit()
    db.refresh(new_job)

    # Send the job to the Celery worker
    process_url.delay(str(new_job.id), str(request.url))

    return {"job_id": new_job.id, "status": new_job.status}


@app.get("/ingest-url/status/{job_id}", response_model=IngestResponse)
def get_ingestion_status(job_id: uuid.UUID, db: Session = Depends(get_db)):
    """
    Retrieves the status of an ingestion job.
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job.id, "status": job.status}


@app.post("/query", response_model=QueryResponse)
def query_knowledge_base(request: QueryRequest):
    """
    Queries the ingested knowledge base to get a grounded answer.
    """
    # 1. Embed the user's query
    query_embedding = embedding_model.encode([request.query])[0]

    # 2. Search ChromaDB for relevant document chunks
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=5  # Retrieve top 5 most relevant chunks
    )
    if not results['documents'] or not results['documents'][0]:
        return {"answer": "I couldn't find any relevant information.", "source_urls": []}
    context_chunks = results['documents'][0]

    source_urls = list(set(meta['url'] for meta in results['metadatas'][0]))

    if not context_chunks:
        return {"answer": "I couldn't find any relevant information.", "source_urls": []}

    # 3. Construct a prompt for the LLM
    prompt = f"""
    You are a helpful AI assistant. Answer the user's question based ONLY on the context provided below.
    If the answer is not in the context, say "I do not have enough information to answer that."

    ---
    CONTEXT:
    {"\n---\n".join(context_chunks)}
    ---

    QUESTION:
    {request.query}

    ANSWER:
    """

    # 4. Generate a grounded answer using Gemini
    try:
        response = llm.generate_content(prompt)
        return {"answer": response.text, "source_urls": source_urls}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate answer: {e}")