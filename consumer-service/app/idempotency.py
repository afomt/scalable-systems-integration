import hashlib

processed = set()

def generate_hash(data):
    return hashlib.sha256(str(data).encode()).hexdigest()

def is_duplicate(data):
    return generate_hash(data) in processed

def mark_processed(data):
    processed.add(generate_hash(data))
