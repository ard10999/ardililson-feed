from collections import defaultdict

from atproto import models

from server.logger import logger
from server.database import db, Post


def operations_callback(ops: defaultdict) -> None:
    # Here we can filter, process, run ML classification, etc.
    # After our feed alg we can save posts into our DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    # for example, let's create our custom feed that will contain all posts that contains alf related text

    posts_to_create = []
    for created_post in ops[models.ids.AppBskyFeedPost]['created']:
        try:
            author = created_post['author']
            record = created_post['record']

            if 'pt' in record.langs:
                if not record.reply:
                    post_dict = {
                        'uri': created_post['uri'],
                        'cid': created_post['cid'],
                        'reply_parent': None,
                        'reply_root': None
                    }
                    posts_to_create.append(post_dict)
        except:
            pass

    posts_to_delete = ops[models.ids.AppBskyFeedPost]['deleted']
    if posts_to_delete:
        post_uris_to_delete = [post['uri'] for post in posts_to_delete]
        Post.delete().where(Post.uri.in_(post_uris_to_delete))

    if posts_to_create:
        with db.atomic():
            for post_dict in posts_to_create:
                Post.create(**post_dict)
