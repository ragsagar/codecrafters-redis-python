import uuid


def generate_repl_id():
    return uuid.uuid4().hex


def is_bigger_stream_id(id1, id2):
    id1_millisecs, id1_seq = id1.split("-")
    id2_millisecs, id2_seq = id2.split("-")
    if id1_millisecs == id2_millisecs:
        return int(id1_seq) > int(id2_seq)
    return int(id1_millisecs) > int(id2_millisecs)
