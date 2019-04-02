import jwt
import json

from tornado import httpclient
from jwt.algorithms import RSAAlgorithm

# Authentication data key
AUTH_DATA = 'auth_data'


def authenticated(handler_class):
    ''' Handle Tornado HTTP Bearer authentication using keycloak '''
    def wrap_execute(handler_execute):

        def require_auth(handler, kwargs):
            # Check if bearer is present in authorization header
            header = handler.request.headers.get('Authorization')
            if header is None or not header.startswith('bearer '):
                return _unauthorized(401, handler)

            # Retrieve JWK from server
            # JWK contains public key that is used for decode JWT token
            # Only keycloak server know private key and can generate tokens
            bearer = header.split(' ')[1]
            try:
                request = httpclient.HTTPRequest(
                    handler.application.config['sso_certs_url'],
                    method='GET',
                )
                response = httpclient.HTTPClient().fetch(request, raise_error=False)
                if response.code == 200:
                    jwk = json.loads(response.body.decode('utf-8'))
                    public_key = RSAAlgorithm.from_jwk(json.dumps(jwk['keys'][0]))
                    auth_data = jwt.decode(bearer, public_key, algorithms='RS256', options={'verify_aud': False})
                    httpclient.HTTPClient().close()
                else:
                    httpclient.HTTPClient().close()
                    return _unauthorized(401, handler)

            except jwt.ExpiredSignatureError:
                return _unauthorized(401, handler)

            # Store connected authentication data in the handler
            handler.request.headers.add(AUTH_DATA, auth_data)

            return True

        def _execute(self, transforms, *args, **kwargs):
            if not require_auth(self, kwargs):
                return False
            return handler_execute(self, transforms, *args, **kwargs)

        return _execute

    handler_class._execute = wrap_execute(handler_class._execute)
    return handler_class


def _unauthorized(code, handler):
    ''' Return a HTTP error  '''
    handler.set_status(code)
    handler.write("Unauthorized" if code == 401 else "Forbidden")
    handler.finish()

    return False


def _checkRole(user_roles, roles):
    ''' Check given role is inside or equals to user roles '''
    if user_roles is None:
        return False
    # Roles is a list not a single element
    if isinstance(roles, list):
        found = False
        for r in roles:
            if r in user_roles:
                found = True
                break

        return found

    # Role is a single string
    else:
        return roles in user_roles

    return False


def roles(roles = None):
    ''' Function decorator to check user role and allow access '''
    def decorator(func):
        def decorated(self, *args, **kwargs):
            auth_data = self.request.headers.get(AUTH_DATA)

            if auth_data is None:
                return _unauthorized(401, self)

            user_realm_roles = auth_data['realm_access']['roles']
            # Check role if necessary
            if roles is not None and not _checkRole(user_realm_roles, roles):
                return _unauthorized(403, self)

            return func(self, *args, **kwargs)
        return decorated
    return decorator
