import asana
import singer

LOGGER = singer.get_logger()


""" Simple wrapper for Asana. """


class Asana():
    """Base class for tap-asana"""

    def __init__(
        self, client_id, client_secret, redirect_uri, refresh_token, access_token=None, options=None
    ):  # pylint: disable=too-many-arguments
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.refresh_token = refresh_token
        self.access_token = access_token
        self._client = self._access_token_auth()

        if options is not None:
            self.update_options(options)

    def _access_token_auth(self):
        """Check for access token"""
        if self.access_token is None:
            LOGGER.debug("OAuth authentication unavailable.")
            return None
        return asana.Client.access_token(self.access_token)

    def update_options(self, options):
        self._client.options.update(options)

    @property
    def client(self):
        return self._client
