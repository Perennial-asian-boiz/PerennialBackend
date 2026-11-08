"""social_posts.score on the [-1, 1] scale; social_sentiment 0 when there are no posts.

social_posts.score:
The sentiment pipeline now scores on [-1, 1] (bullish minus bearish
probability) instead of 1-10. social_posts.score becomes numeric(5,4) checked to
[-1, 1], and every stored score is converted with the old pipeline's own
mapping, (score - 5.5) / 4.5, so 1 becomes -1, 5.5 becomes 0 and 10 becomes 1.

ticker_sentiment.social_sentiment: a ticker with no social posts now stores 0
(not NULL), so ck_ticker_sentiment_social_sentiment_counted also accepts
social_count = 0 with social_sentiment = 0.

Downgrade converts the scores back with score * 4.5 + 5.5, rounded to two
places, and turns those zeros back into NULL before restoring 0023's check.
"""
from alembic import op

revision = '0024'
down_revision = '0023'
branch_labels = None
depends_on = None

TABLE = 'perennial.social_posts'
CHECK = 'ck_social_posts_score'
SENTIMENT = 'perennial.ticker_sentiment'
COUNTED = 'ck_ticker_sentiment_social_sentiment_counted'


def upgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f'ALTER TABLE {TABLE} DROP CONSTRAINT {CHECK}')
    op.execute(f'ALTER TABLE {TABLE} ALTER COLUMN score TYPE numeric(5, 4) '
               'USING round((score - 5.5) / 4.5, 4)')
    op.execute(f'ALTER TABLE {TABLE} ADD CONSTRAINT {CHECK} CHECK (score >= -1 AND score <= 1)')
    op.execute(f'ALTER TABLE {SENTIMENT} DROP CONSTRAINT {COUNTED}')
    op.execute(f'ALTER TABLE {SENTIMENT} ADD CONSTRAINT {COUNTED} CHECK '
               '(social_sentiment IS NULL OR social_count > 0 OR social_sentiment = 0)')


def downgrade():
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f'ALTER TABLE {SENTIMENT} DROP CONSTRAINT {COUNTED}')
    op.execute(f'UPDATE {SENTIMENT} SET social_sentiment = NULL WHERE social_count = 0')
    op.execute(f'ALTER TABLE {SENTIMENT} ADD CONSTRAINT {COUNTED} CHECK '
               '(social_sentiment IS NULL OR social_count > 0)')
    op.execute(f'ALTER TABLE {TABLE} DROP CONSTRAINT {CHECK}')
    op.execute(f'ALTER TABLE {TABLE} ALTER COLUMN score TYPE numeric(4, 2) '
               'USING round(score * 4.5 + 5.5, 2)')
    op.execute(f'ALTER TABLE {TABLE} ADD CONSTRAINT {CHECK} CHECK (score >= 1 AND score <= 10)')
