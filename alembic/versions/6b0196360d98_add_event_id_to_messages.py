"""Add event_id to messages

Revision ID: 6b0196360d98
Revises: 1ab87aa1a445
Create Date: 2026-09-11 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '6b0196360d98'
down_revision: Union[str, Sequence[str], None] = '1ab87aa1a445'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Nullable, no default, no constraint - purely additive. Dedup
    # enforcement (unique constraint / ON CONFLICT) is deferred; see
    # db/models.py's Message.event_id comment.
    op.add_column('messages', sa.Column('event_id', sa.Text(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('messages', 'event_id')
