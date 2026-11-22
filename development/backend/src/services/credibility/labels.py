"""The three sentiment labels and the score cutoffs between them.

One rule for every scored item, so the Sentiment Pulse's News and Social bars
mean the same thing (S3-02): a score in [-1, 1] is negative at or below
NEGATIVE_AT_MOST, positive at or above POSITIVE_AT_LEAST, and neutral between.
The article scorer stores it as article_sentiment.label (0022); the social
import stores it as social_posts.label (0026), whose CHECK repeats these numbers
(models.LABEL_OF_SCORE, and migration 0026; a test keeps them equal).
"""

from decimal import Decimal

LABELS = ("negative", "neutral", "positive")
NEGATIVE_AT_MOST = Decimal("-0.2")  # score <= this: negative
POSITIVE_AT_LEAST = Decimal("0.2")  # score >= this: positive; between: neutral


def label_for(score: Decimal) -> str:
    """negative in [-1, -0.2], neutral in (-0.2, 0.2), positive in [0.2, 1]."""
    if score <= NEGATIVE_AT_MOST:
        return "negative"
    if score >= POSITIVE_AT_LEAST:
        return "positive"
    return "neutral"
