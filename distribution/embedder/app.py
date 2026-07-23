import os

from fastapi import FastAPI
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer

MODEL_ID = os.environ.get("MODEL_ID", "BAAI/bge-large-en-v1.5")
model = SentenceTransformer(MODEL_ID)
api = FastAPI()


class EmbedRequest(BaseModel):
    texts: list[str] = Field(min_length=1, max_length=32)


@api.get("/health")
def health() -> dict[str, object]:
    return {"status": "ok", "model": MODEL_ID, "dims": 1024}


@api.post("/embed")
def embed(request: EmbedRequest) -> dict[str, object]:
    vectors = model.encode(
        request.texts,
        normalize_embeddings=True,
        convert_to_numpy=True,
    ).tolist()
    return {
        "embeddings": vectors,
        "model": MODEL_ID,
        "dims": 1024,
        "count": len(vectors),
    }
