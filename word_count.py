import re
import string

def map(key, value):
    """
    Mapper function for word counting.
    
    Args:
        key: Document ID/key
        value: Document text content
        
    Returns:
        List of (word, 1) pairs
    """
    # Normalize text
    text = value.lower()
    
    # Remove punctuation
    translator = str.maketrans('', '', string.punctuation)
    text = text.translate(translator)
    
    # Split into words
    words = text.split()
    
    # Emit (word, 1) pairs
    return [(word, 1) for word in words]
