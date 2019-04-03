import jwt
import json
import datetime

from tornado import httpclient
from jwt.algorithms import RSAAlgorithm
from urllib.parse import urlencode

# Authentication data key
AUTH_DATA = 'auth_data'


def authenticated(handler_class):
    """ Handle Tornado HTTP Bearer authentication using keycloak
    This decorator can be used on class like the following sample:
        @authenticated
        class MainHandler(factornado.RequestHandler):
            def get(self):
                self.write('Only authenticated')
    """
    def wrap_execute(handler_execute):
        def _execute(self, transforms, *args, **kwargs):
            if not _check_auth(self, kwargs):
                return False
            return handler_execute(self, transforms, *args, **kwargs)

        return _execute

    handler_class._execute = wrap_execute(handler_class._execute)
    return handler_class


def roles(roles=None, clientId=None):
    """ Function decorator to check user role and allow access
    Combinate with the authentication decorator,
    this decorator can be used on method like the following sample:
        @authenticated
        class MainHandler(factornado.RequestHandler):
            @roles('admin')
            def get(self):
                self.write('Only admin users')
    By default, realm roles are checked
    If you want client roles don't forget to specify sso_client_id in the config
    """
    def decorator(func):
        def decorated(self, *args, **kwargs):
            auth_data = self.request.headers.get(AUTH_DATA)

            if auth_data is None:
                return _unauthorized(401, self)

            user_realm_roles = auth_data['realm_access']['roles']
            # Check role if necessary
            if roles is not None and not _checkRole(user_realm_roles, roles):
                clientId = self.application.config['sso']['clientId']
                if clientId is not None and auth_data['resource_access'][clientId] is not None:
                    user_client_roles = auth_data['resource_access'][clientId]['roles']
                    if not _checkRole(user_client_roles, roles):
                        return _unauthorized(403, self)
                else:
                    return _unauthorized(403, self)

            return func(self, *args, **kwargs)
        return decorated
    return decorator


def get_token(application):
    """ Use SSO server to get JWT token
        Take care, to get a new token, service need to be declare in sso
        as confidential client with direct access grants enabled

        In final service, the following sample allow to create a new token
        and call other service with it :

        from factornado.authentication import get_token
        ...
        token = get_token(application)
        request = httpclient.HTTPRequest(
            url,
            method='POST',
            headers={'Authorization': 'bearer {}'.format(token)},
            body=(parameters)
        )
    """
    sso = application.config['sso']
    if not sso:
        return None

    # Check if a token exists and verify validity
    token = application.config.get('token', None)
    now = datetime.datetime.now().timestamp()
    if (token is not None and jwt.decode(token, verify=False)['exp'] > now):
        return token
    else:
        url = '{}realms/{}/protocol/openid-connect/token'.format(sso['url'], sso['realm'])
        parameters = urlencode({
            'grant_type': 'client_credentials',
            'client_id': sso['client_id'],
            'client_secret': sso['client_secret']
        })
        application.logger.debug(
            '[SSO] Get token on {} for client : {} '.format(url, sso['client_id'])
        )
        # Retrieve token using client credentials
        request = httpclient.HTTPRequest(
            url,
            method='POST',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            body=(parameters)
        )
        response = httpclient.HTTPClient().fetch(request, raise_error=False)

        if response.code == 200:
            token = json.loads(response.body.decode('utf-8'))['access_token']
            application.config['token'] = token
        else:
            token = None
        httpclient.HTTPClient().close()

        return token


def _unauthorized(code, handler):
    """ Return a HTTP error  """
    handler.set_status(code)
    handler._transforms = []
    handler.write("Unauthorized" if code == 401 else "Forbidden")
    handler.finish()

    return False


def _check_auth(handler, kwargs):
    """ Check authentication using bearer and sso server """
    # Check if bearer is present in authorization header
    header = handler.request.headers.get('Authorization')
    sso = handler.application.config['sso']

    if header is None or not header.startswith('bearer ') or not sso:
        return _unauthorized(401, handler)

    # Retrieve JWK from server
    # JWK contains public key that is used for decode JWT token
    # Only keycloak server know private key and can generate tokens
    # For more flexibility it possible to use .weel-known url
    # to retrieve all realm openid configuration
    # /auth/realms/fleetscience/.well-known/openid-configuration
    bearer = header.split(' ')[1]

    try:
        handler.application.logger.debug(
            '[SSO] Check authentication for realm {}'.format(sso['realm'])
        )
        request = httpclient.HTTPRequest(
            '{}realms/{}/protocol/openid-connect/certs'.format(sso['url'], sso['realm']),
            method='GET',
        )
        response = httpclient.HTTPClient().fetch(request, raise_error=False)
        if response.code == 200:
            jwk = json.loads(response.body.decode('utf-8'))
            public_key = RSAAlgorithm.from_jwk(json.dumps(jwk['keys'][0]))
            auth_data = jwt.decode(bearer,
                                   public_key,
                                   algorithms=jwk['keys'][0]['alg'],
                                   options={'verify_aud': False})
            httpclient.HTTPClient().close()
        else:
            httpclient.HTTPClient().close()
            return _unauthorized(401, handler)

    except jwt.ExpiredSignatureError:
        return _unauthorized(401, handler)

    # Store connected authentication data in the handler
    handler.request.headers.add(AUTH_DATA, auth_data)

    return True


def _checkRole(user_roles, roles):
    """ Check given role is inside or equals to user roles """
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
