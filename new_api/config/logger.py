from elasticapm.contrib.starlette import make_apm_client
from .setting import env

apm = make_apm_client({
    'SERVICE_NAME': env.apm_service_name,
    'SERVER_URL': env.apm_server_url,
    'TRANSACTIONS_IGNORE_PATTERNS': '/ws*',
    'ENVIRONMENT': env.app_env,
    'CAPTURE_BODY': 'all',
    'SANITIZE_FIELD_NAMES': (
        "password",
        "passwd",
        "pwd",
        "secret",
        "*key",
        "*token*",
        "*credit*",
        "*card*",
        "*auth*",
        "set-cookie",
    ),
})