"""init

Revision ID: cb1fc9087081
Revises: 
Create Date: 2024-06-27 13:40:42.995874

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cb1fc9087081'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('buildings',
    sa.Column('title', sa.String(), nullable=False),
    sa.Column('pyramid_title', sa.String(), nullable=True),
    sa.Column('external_id', sa.Integer(), nullable=False),
    sa.Column('id', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('external_id')
    )
    op.create_table('tags',
    sa.Column('title', sa.String(), nullable=False),
    sa.Column('description', sa.String(), nullable=False),
    sa.Column('id', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('devices',
    sa.Column('full_title', sa.String(), nullable=False),
    sa.Column('guid', sa.String(), nullable=False),
    sa.Column('sync_id', sa.Integer(), nullable=False),
    sa.Column('serial_number', sa.String(), nullable=False),
    sa.Column('building_external_id', sa.Integer(), nullable=False),
    sa.Column('id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['building_external_id'], ['buildings.external_id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('guid'),
    sa.UniqueConstraint('sync_id'),
    sa.UniqueConstraint('sync_id', name='uq_sync_id')
    )
    op.create_table('data',
    sa.Column('value', sa.Float(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('tag_id', sa.Integer(), nullable=False),
    sa.Column('device_sync_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['device_sync_id'], ['devices.sync_id'], ),
    sa.ForeignKeyConstraint(['tag_id'], ['tags.id'], ),
    sa.PrimaryKeyConstraint('created_at', 'tag_id', 'device_sync_id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('data')
    op.drop_table('devices')
    op.drop_table('tags')
    op.drop_table('buildings')
    # ### end Alembic commands ###
