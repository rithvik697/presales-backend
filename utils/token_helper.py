import jwt
from flask import request
from config import PUBLIC_KEY, JWT_ISSUER, JWT_AUDIENCE
import logging

logger = logging.getLogger(__name__)


def get_emp_id_from_token():
    """
    Reads the Authorization: Bearer <token> header from the current request,
    verifies and decodes the JWT using the RS256 public key, and returns the
    employee ID stored in the 'sub' claim.

    Returns:
        str: The emp_id of the logged-in user (e.g. 'EMP004'), or None if the
             token is missing, invalid, or expired.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        logger.warning("Missing or malformed Authorization header")
        return None

    token = auth_header.split(" ", 1)[1]
    try:
        # Build decode options — only validate iss/aud if they are actually configured
        options = {}
        decode_kwargs = {
            "algorithms": ["RS256"],
        }
        if JWT_ISSUER:
            decode_kwargs["issuer"] = JWT_ISSUER
        if JWT_AUDIENCE:
            decode_kwargs["audience"] = JWT_AUDIENCE

        payload = jwt.decode(token, PUBLIC_KEY, **decode_kwargs)
        emp_id = payload.get("sub")
        logger.info(f"Token decoded successfully for emp_id={emp_id}")
        return emp_id

    except jwt.ExpiredSignatureError:
        logger.warning("JWT token has expired — user must log in again")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid JWT token: {e}")
        return None
