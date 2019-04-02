import jwt
import json

from tornado import httpclient
from jwt.algorithms import RSAAlgorithm

def auth(handler_class):
    ''' Handle Tornado HTTP Bearer authentication using keycloak '''
    def wrap_execute(handler_execute):

        def _unauthorized(handler):
			handler.set_status(401)
			handler.write("Unauthorized")
			handler.finish()

			return False

        def require_auth(handler, kwargs):
            # Check if bearer is present in authorization header
            header = handler.request.headers.get('Authorization')
            if header is None or not header.startswith('bearer '):
                return _unauthorized(handler)
 
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
                else:
                    return _unauthorized(handler)

                httpclient.HTTPClient().close()

            except jwt.ExpiredSignatureError:
                return _unauthorized(handler)

            # Store connected authentication data in the handler
            handler.request.headers.add('auth_data', auth_data)
 
            return True
 
        def _execute(self, transforms, *args, **kwargs):
            if not require_auth(self, kwargs):
                return False
            return handler_execute(self, transforms, *args, **kwargs)
 
        return _execute
 
    handler_class._execute = wrap_execute(handler_class._execute)
    return handler_class