"""Remove api_key and result_url columns

Revision ID: 84583fc5153e
Revises:
Create Date: 2023-11-10 17:10:10.636653

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '84583fc5153e'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    context = op.get_context()
    inspect = sa.inspect(context.bind)
    columns = inspect.get_columns('jobs')
    column_names = [c['name'] for c in columns]
    if 'result_url' in column_names:
        op.drop_column('jobs', 'result_url')
    if 'api_key' in column_names:
        op.drop_column('jobs', 'api_key')


def downgrade():
    op.add_column('jobs', sa.Column('result_url', sa.UnicodeText))
    op.add_column('jobs', sa.Column('api_key', sa.UnicodeText))
