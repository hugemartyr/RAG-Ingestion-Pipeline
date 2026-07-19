import re
from typing import List

def chunk_document(
    text: str, 
    max_chunk_size: int = 500, 
    overlap: int = 0
) -> List[str]:
    """
    Splits text by sentence boundaries (., ?, !) and packs sentences into chunks
    up to max_chunk_size characters. If overlap is specified, some trailing sentences
    from the previous chunk can be carried over.
    """
    if not text:
        return []
    
    # Split text into sentences using lookbehind-based regex to keep punctuation
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    
    chunks = []
    current_chunk = []
    current_length = 0
    
    for sentence in sentences:
        sentence_len = len(sentence)
        
        # Handle sentences that exceed max_chunk_size on their own
        if sentence_len > max_chunk_size:
            if current_chunk:
                chunks.append(" ".join(current_chunk))
                current_chunk = []
                current_length = 0
            chunks.append(sentence)
            continue
            
        # Check if adding the sentence exceeds the max size limit
        space_padding = len(current_chunk) > 0
        if current_length + space_padding + sentence_len > max_chunk_size:
            chunks.append(" ".join(current_chunk))
            
            # Simple sentence-level overlap carryover
            if overlap > 0 and current_chunk:
                overlap_chunk = []
                overlap_len = 0
                for s in reversed(current_chunk):
                    s_padding = len(overlap_chunk) > 0
                    if overlap_len + s_padding + len(s) <= overlap:
                        overlap_chunk.insert(0, s)
                        overlap_len += len(s) + (1 if s_padding else 0)
                    else:
                        break
                current_chunk = overlap_chunk
                current_length = overlap_len
            else:
                current_chunk = []
                current_length = 0
        
        space_padding = len(current_chunk) > 0
        current_chunk.append(sentence)
        current_length += sentence_len + (1 if space_padding else 0)
        
    if current_chunk:
        chunks.append(" ".join(current_chunk))
        
    return chunks
