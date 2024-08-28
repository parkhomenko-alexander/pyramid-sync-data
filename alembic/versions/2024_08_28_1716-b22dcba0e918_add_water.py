"""add water

Revision ID: b22dcba0e918
Revises: dac27c5bc59d
Create Date: 2024-08-28 17:16:17.584915

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b22dcba0e918'
down_revision: Union[str, None] = 'dac27c5bc59d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('water',
    sa.Column('electro_unit', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('plan', sa.Float(), nullable=False),
    sa.Column('fact', sa.Float(), nullable=False),
    sa.Column('plane_coast', sa.Float(), nullable=False),
    sa.Column('fact_coast', sa.Float(), nullable=False),
    sa.Column('delta_values', sa.Float(), nullable=False),
    sa.Column('delta_coast', sa.Float(), nullable=False),
    sa.Column('id', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('water')
    # ### end Alembic commands ###
