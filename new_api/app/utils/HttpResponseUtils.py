from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from starlette.exceptions import HTTPException

def response_error(error, msg = 'Terjadi kesalahan, silahkan coba lagi', code = 400, data = None):
    _msg = error if '[WARN]' in error else msg
    raise HTTPException(
        code,
        {
            "status": 0,
            "msg": _msg,
            "data": data,
            "error": error
        },
    )

def response_format(msg, code, data = None):
    return JSONResponse(status_code = code, content = jsonable_encoder({
        "status": 0,
        "data": data,
        "message": msg
    }))

def response_success(data, message_chunk = "", msg = "Success"):
    return JSONResponse(status_code=200, content=jsonable_encoder({
        "status": 1,
        "data": jsonable_encoder(data),
        "message": msg
    }))

def response_success_sse(data, message_chunk = "", msg = "Success"):
    return {
        "status": 1,
        "message_chunk": message_chunk,
        "data": jsonable_encoder(data),
        "message": msg
    }