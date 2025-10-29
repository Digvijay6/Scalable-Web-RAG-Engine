import celery
import uuid
import chromadb
import requests
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
from database import SessionLocal, engine
from models import IngestionJob, JobStatus, Base
from config import REDIS_URL

# Create table if it doesn't exist
Base.metadata.create_all(bind=engine)

# Initialize Celery
celery_app = celery.Celery("worker", broker=REDIS_URL)

# Initialize models and DB
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
# This creates a persistent DB on disk in the "./chroma_db" folder
chroma_client = chromadb.PersistentClient(path="./chroma_db")
# Use a simple collection for all URLs. For production, you might create one per user/job.
collection = chroma_client.get_or_create_collection(name="web_content")

@celery_app.task
def process_url(job_id: str, url: str):
    job_id_uuid = uuid.UUID(job_id)
    db = SessionLocal()
    try:
        # 1. Update status to PROCESSING
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id_uuid).first()
        if not job:
            return
        job.status = JobStatus.PROCESSING
        db.commit()

        # 2. Fetch and clean web content
        print(f"Processing URL: {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        # Simple text extraction: get all paragraph texts
        texts = [p.get_text() for p in soup.find_all('p')]
        content = "\n".join(texts)

        # 3. Chunk the text (simple splitting for demo)
        chunks = [content[i:i + 500] for i in range(0, len(content), 400)] # Overlapping chunks

        # 4. Generate embeddings and store in ChromaDB
        if chunks:
            embeddings = embedding_model.encode(chunks)
            ids = [f"{job_id}_{i}" for i, _ in enumerate(chunks)]
            metadata = [{"url": url} for _ in chunks]

            collection.add(
                embeddings=embeddings,
                documents=chunks,
                metadatas=metadata,
                ids=ids
            )
            print(f"Stored {len(chunks)} chunks for job {job_id}")

        # 5. Update status to COMPLETED
        job.status = JobStatus.COMPLETED
        db.commit()

    except Exception as e:
        print(f"Error processing {url}: {e}")
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id_uuid).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            db.commit()
    finally:
        db.close()