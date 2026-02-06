import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PRIVATE_KEY_PATH = os.path.join(BASE_DIR, "private.pem")
PUBLIC_KEY_PATH = os.path.join(BASE_DIR, "public.pem")

with open(PRIVATE_KEY_PATH, "r") as f:
    PRIVATE_KEY = f.read()

with open(PUBLIC_KEY_PATH, "r") as f:
    PUBLIC_KEY = f.read()

JWT_ISSUER = os.getenv("JWT_ISSUER")
JWT_AUDIENCE = os.getenv("JWT_AUDIENCE")

if not PRIVATE_KEY or not PUBLIC_KEY:
    raise RuntimeError("JWT keys not configured")
