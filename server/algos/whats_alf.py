from datetime import datetime
from typing import Optional

from server import config
from server.database import Post
from server.logger import logger

uri = config.WHATS_ALF_URI
CURSOR_EOF = 'eof'


def handler(cursor: Optional[str], limit: int) -> dict:
    posts = Post.select().order_by(Post.id.desc()).limit(limit)
    if cursor:
        if cursor == CURSOR_EOF:
            return {
                'cursor': CURSOR_EOF,
                'feed': []
            }
        cursor_parts = cursor.split('::')
        if len(cursor_parts) != 2:
            raise ValueError('Malformed cursor')

        indexed_at, cid = cursor_parts
        indexed_at = datetime.fromtimestamp(int(indexed_at) / 1000)
        posts = posts.where(((Post.indexed_at == indexed_at) & (Post.cid < cid)) | (Post.indexed_at < indexed_at))

    feed = [{'post': post.uri} for post in posts]

    last_post = posts[-1] if posts else None
    if last_post:
        logger.info(last_post)
        cursor = f'{int(last_post.indexed_at.timestamp() * 1000)}::{last_post.cid}'
        logger.info(cursor)

    return {
        'cursor': cursor,
        'feed': feed
    }
