from dotenv import load_dotenv
import os

load_dotenv() # Load variables from .env file

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")