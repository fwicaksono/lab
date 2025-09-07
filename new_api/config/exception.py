from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from config.logger import apm
from app.utils.HttpResponseUtils import response_format

def setup_exception(app):
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request, exc):
        apm.capture_exception(custom={"detail": str(exc)})
        detail = exc.detail if isinstance(exc.detail, dict) else { "msg": None, "data": None }
        return response_format(detail.get('msg'),exc.status_code, detail.get('data'))

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request, exc):
        apm.capture_exception(custom={"detail": str(exc)})
        return response_format("Terjadi kesalahan, silahkan coba lagi",400, str(exc))
