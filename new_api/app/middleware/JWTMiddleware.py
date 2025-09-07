from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends, HTTPException, status
from typing import Annotated
from config.setting import env

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class JWTMiddleware:
    def __init__(self):
        self.secret_key = env.secret_key
        self.algorithm = "HS256"

    async def validate_token(self, token: Annotated[str, Depends(oauth2_scheme)]):
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            print("payload:", payload)
            
            # Customize these validation rules for your medical search API
            source = payload.get("source")
            app_type = payload.get("app_type")
            
            print("source:", source)
            print("app_type:", app_type)
            
            # Update these validation rules for your specific use case
            if source != "MEDICAL_SEARCH" or app_type != "API":
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail={
                        "status": 0,
                        "msg": "Could not validate credentials",
                        "data": None,
                        "error": "Invalid token source or app type"
                    }
                )
            return payload
        except JWTError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "status": 0,
                    "msg": "Could not validate credentials", 
                    "data": None,
                    "error": str(e)
                }
            )

jwtMiddleware = JWTMiddleware()