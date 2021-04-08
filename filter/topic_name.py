def topic_name(userid, name):
    """topic_name.

    Args:
        userid:
        name:
    """
    name =  ''.join(e for e in name if e.isalnum() or e=='_' or e=='-' or e=='.')
    return '%d'%userid + name
