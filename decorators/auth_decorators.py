from functools import wraps
from flask import request, jsonify
import jwt
from config import PUBLIC_KEY, JWT_ISSUER, JWT_AUDIENCE

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization")

        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"message": "Authorization token missing"}), 401

        token = auth_header.split(" ")[1]

        try:
            decoded = jwt.decode(
                token,
                PUBLIC_KEY,
                algorithms=["RS256"],
                issuer=JWT_ISSUER,
                audience=JWT_AUDIENCE
            )
        except jwt.ExpiredSignatureError:
            return jsonify({"message": "Token expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"message": "Invalid token"}), 401

        # pass decoded token to the route
        return f(decoded, *args, **kwargs)

    return decorated
