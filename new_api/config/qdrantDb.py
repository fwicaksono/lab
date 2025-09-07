import uuid
import qdrant_client
from datetime import datetime
from qdrant_client import models
from qdrant_client.models import Distance, PointStruct

from .setting import env
from app.services.QdrantServices import Qdrant
from app.services.EmbeddingServices import embed

class QdrantDB:
    def __init__(self):
        self.client = qdrant_client.QdrantClient(
            url=env.qdrant_host,
            api_key=env.qdrant_api_key,
        )

    async def get_db(self, collection, distance=Distance.DOT):
        return Qdrant(
            self.client,
            collection_name=collection,
            distance_strategy=distance,
            embedding_function=embed.embed_query_intfloat
        )

    async def get_retriever_infloat(self, collection, **kwargs):
        db = await self.get_db(collection, Distance.COSINE)
        retriever = db.as_retriever(**kwargs)
        return retriever

db = QdrantDB()

def get_qdrant_db():
    return db


def get_qdrant_client():
    return db.client
