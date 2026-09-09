from livekit import api

from core.config import settings


def create_room_token(*, identity: str, room_name: str) -> str:
    """Issue a JWT that lets `identity` publish/subscribe audio in `room_name`.

    This is the only piece of the control plane that talks to LiveKit for
    media access — FastAPI never touches the audio itself, it just hands the
    client a token and the client connects to LiveKit's SFU directly.
    """
    grants = api.VideoGrants(room_join=True, room=room_name, can_publish=True, can_subscribe=True)
    token = (
        api.AccessToken(settings.livekit_api_key, settings.livekit_api_secret)
        .with_identity(identity)
        .with_grants(grants)
    )
    return token.to_jwt()
