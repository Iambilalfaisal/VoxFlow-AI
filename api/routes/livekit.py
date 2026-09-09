import uuid

from fastapi import APIRouter
from pydantic import BaseModel

from api.dependencies import CurrentUser, DbSession
from core.config import settings
from db.models import Conversation
from services.livekit_service import create_room_token

router = APIRouter(prefix="/livekit")


class RoomTokenResponse(BaseModel):
    livekit_url: str
    room_name: str
    access_token: str


@router.post("/token", response_model=RoomTokenResponse)
async def issue_room_token(db: DbSession, user: CurrentUser) -> RoomTokenResponse:
    """Start a new conversation and hand back what the client needs to join
    the LiveKit room directly — FastAPI's involvement ends here."""
    room_name = f"voxflow-{uuid.uuid4()}"

    conversation = Conversation(user_id=user.id, livekit_room_name=room_name)
    db.add(conversation)
    await db.commit()

    token = create_room_token(identity=str(user.id), room_name=room_name)
    return RoomTokenResponse(livekit_url=settings.livekit_url, room_name=room_name, access_token=token)
