from starlette.middleware.cors import CORSMiddleware
from config.setting import env

origins = env.allowed_origins.split(",")

def setup_middleware(app):
    app.add_middleware(
        CORSMiddleware, 
        allow_origins=origins, 
        allow_credentials=True, 
        allow_methods=["GET", "POST", "OPTIONS"]
    )