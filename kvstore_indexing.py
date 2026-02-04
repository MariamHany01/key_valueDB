"""
Indexing Module for KVStore
Implements:
1. Inverted Index (Full-text search)
2. Word Embeddings (Semantic search)
"""

import re
from typing import Dict, List, Set, Any, Tuple
from collections import defaultdict
import pickle
from pathlib import Path
import threading


class InvertedIndex:
    """Inverted index for full-text search on values"""
    
    def __init__(self):
        self.index: Dict[str, Set[str]] = defaultdict(set)  # word -> set of keys
        self._lock = threading.RLock()
    
    def tokenize(self, text: str) -> List[str]:
        """Tokenize text into words"""
        if not isinstance(text, str):
            text = str(text)
        
        # Convert to lowercase and split on non-alphanumeric
        words = re.findall(r'\w+', text.lower())
        return words
    
    def index_document(self, key: str, value: Any):
        """Index a document (key-value pair)"""
        with self._lock:
            # Remove old index entries for this key
            self.remove_document(key)
            
            # Tokenize and index
            tokens = self.tokenize(value)
            for token in tokens:
                self.index[token].add(key)
    
    def remove_document(self, key: str):
        """Remove a document from the index"""
        with self._lock:
            # Remove key from all word sets
            for word in list(self.index.keys()):
                if key in self.index[word]:
                    self.index[word].discard(key)
                    if not self.index[word]:
                        del self.index[word]
    
    def search(self, query: str) -> Set[str]:
        """Search for keys matching the query"""
        with self._lock:
            tokens = self.tokenize(query)
            
            if not tokens:
                return set()
            
            # Get keys containing all tokens (AND search)
            result_sets = [self.index.get(token, set()) for token in tokens]
            
            if not result_sets:
                return set()
            
            # Intersection of all sets
            results = result_sets[0].copy()
            for s in result_sets[1:]:
                results &= s
            
            return results
    
    def search_or(self, query: str) -> Set[str]:
        """Search for keys matching any token (OR search)"""
        with self._lock:
            tokens = self.tokenize(query)
            
            if not tokens:
                return set()
            
            # Union of all sets
            results = set()
            for token in tokens:
                results |= self.index.get(token, set())
            
            return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics"""
        with self._lock:
            return {
                'unique_words': len(self.index),
                'total_entries': sum(len(keys) for keys in self.index.values())
            }


class SimpleWordEmbedding:
    """
    Simple word embedding using character n-grams
    (In production, you'd use pre-trained embeddings like Word2Vec, GloVe, or BERT)
    """
    
    def __init__(self, n: int = 3):
        self.n = n  # n-gram size
        self.embeddings: Dict[str, List[str]] = {}  # key -> list of n-grams
        self._lock = threading.RLock()
    
    def extract_ngrams(self, text: str) -> List[str]:
        """Extract character n-grams from text"""
        if not isinstance(text, str):
            text = str(text)
        
        text = text.lower()
        ngrams = []
        
        for i in range(len(text) - self.n + 1):
            ngrams.append(text[i:i+self.n])
        
        return ngrams
    
    def index_document(self, key: str, value: Any):
        """Create embedding for a document"""
        with self._lock:
            ngrams = self.extract_ngrams(value)
            self.embeddings[key] = ngrams
    
    def remove_document(self, key: str):
        """Remove document embedding"""
        with self._lock:
            if key in self.embeddings:
                del self.embeddings[key]
    
    def jaccard_similarity(self, set1: Set[str], set2: Set[str]) -> float:
        """Calculate Jaccard similarity between two sets"""
        if not set1 or not set2:
            return 0.0
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
    def search(self, query: str, top_k: int = 10, threshold: float = 0.1) -> List[Tuple[str, float]]:
        """
        Search for similar documents using embedding similarity
        Returns list of (key, similarity_score) tuples
        """
        with self._lock:
            query_ngrams = set(self.extract_ngrams(query))
            
            if not query_ngrams:
                return []
            
            # Calculate similarity with all documents
            similarities = []
            for key, doc_ngrams in self.embeddings.items():
                doc_ngram_set = set(doc_ngrams)
                similarity = self.jaccard_similarity(query_ngrams, doc_ngram_set)
                
                if similarity >= threshold:
                    similarities.append((key, similarity))
            
            # Sort by similarity (descending)
            similarities.sort(key=lambda x: x[1], reverse=True)
            
            return similarities[:top_k]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get embedding statistics"""
        with self._lock:
            return {
                'total_documents': len(self.embeddings),
                'ngram_size': self.n
            }


class IndexedKVStore:
    """
    KVStore wrapper with indexing capabilities
    """
    
    def __init__(self, kvstore):
        self.kvstore = kvstore
        self.inverted_index = InvertedIndex()
        self.embedding_index = SimpleWordEmbedding()
        self._lock = threading.RLock()
        
        # Build initial indexes from existing data
        self._rebuild_indexes()
    
    def _rebuild_indexes(self):
        """Rebuild indexes from existing data"""
        print("Building indexes...")
        with self._lock:
            data = self.kvstore.get_all_data()
            for key, value in data.items():
                self.inverted_index.index_document(key, value)
                self.embedding_index.index_document(key, value)
        print(f"Indexes built. Inverted index: {self.inverted_index.get_stats()}")
    
    def set(self, key: str, value: Any) -> bool:
        """Set with automatic indexing"""
        with self._lock:
            result = self.kvstore.set(key, value)
            if result:
                self.inverted_index.index_document(key, value)
                self.embedding_index.index_document(key, value)
            return result
    
    def get(self, key: str) -> Any:
        """Get value"""
        return self.kvstore.get(key)
    
    def delete(self, key: str) -> bool:
        """Delete with index cleanup"""
        with self._lock:
            result = self.kvstore.delete(key)
            if result:
                self.inverted_index.remove_document(key)
                self.embedding_index.remove_document(key)
            return result
    
    def bulk_set(self, items: List[Tuple[str, Any]]) -> bool:
        """Bulk set with indexing"""
        with self._lock:
            result = self.kvstore.bulk_set(items)
            if result:
                for key, value in items:
                    self.inverted_index.index_document(key, value)
                    self.embedding_index.index_document(key, value)
            return result
    
    def search_fulltext(self, query: str, match_all: bool = True) -> List[Tuple[str, Any]]:
        """
        Full-text search
        Args:
            query: Search query
            match_all: If True, match all words (AND). If False, match any word (OR)
        Returns:
            List of (key, value) tuples
        """
        if match_all:
            keys = self.inverted_index.search(query)
        else:
            keys = self.inverted_index.search_or(query)
        
        results = []
        for key in keys:
            value = self.kvstore.get(key)
            if value is not None:
                results.append((key, value))
        
        return results
    
    def search_semantic(self, query: str, top_k: int = 10, threshold: float = 0.1) -> List[Tuple[str, Any, float]]:
        """
        Semantic search using embeddings
        Returns:
            List of (key, value, similarity_score) tuples
        """
        similar_keys = self.embedding_index.search(query, top_k=top_k, threshold=threshold)
        
        results = []
        for key, score in similar_keys:
            value = self.kvstore.get(key)
            if value is not None:
                results.append((key, value, score))
        
        return results
    
    def get_index_stats(self) -> Dict[str, Any]:
        """Get statistics about indexes"""
        return {
            'inverted_index': self.inverted_index.get_stats(),
            'embedding_index': self.embedding_index.get_stats()
        }


# Example usage and tests
if __name__ == '__main__':
    from kvstore_server import KVStore
    import tempfile
    import shutil
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create indexed store
        kvstore = KVStore(temp_dir)
        indexed_store = IndexedKVStore(kvstore)
        
        # Add some sample data
        print("\nAdding sample data...")
        indexed_store.set("doc1", "The quick brown fox jumps over the lazy dog")
        indexed_store.set("doc2", "A fast brown fox leaps over a sleeping dog")
        indexed_store.set("doc3", "The lazy cat sleeps all day long")
        indexed_store.set("doc4", "Machine learning and artificial intelligence")
        indexed_store.set("doc5", "Deep learning neural networks are powerful")
        
        # Full-text search
        print("\n" + "="*60)
        print("FULL-TEXT SEARCH TESTS")
        print("="*60)
        
        print("\nSearch for 'brown fox':")
        results = indexed_store.search_fulltext("brown fox")
        for key, value in results:
            print(f"  {key}: {value}")
        
        print("\nSearch for 'lazy' (OR):")
        results = indexed_store.search_fulltext("lazy", match_all=False)
        for key, value in results:
            print(f"  {key}: {value}")
        
        # Semantic search
        print("\n" + "="*60)
        print("SEMANTIC SEARCH TESTS")
        print("="*60)
        
        print("\nSemantic search for 'quick fox jumping':")
        results = indexed_store.search_semantic("quick fox jumping", top_k=3)
        for key, value, score in results:
            print(f"  {key} (score: {score:.3f}): {value}")
        
        print("\nSemantic search for 'neural network AI':")
        results = indexed_store.search_semantic("neural network AI", top_k=3)
        for key, value, score in results:
            print(f"  {key} (score: {score:.3f}): {value}")
        
        # Index stats
        print("\n" + "="*60)
        print("INDEX STATISTICS")
        print("="*60)
        stats = indexed_store.get_index_stats()
        print(f"Inverted Index: {stats['inverted_index']}")
        print(f"Embedding Index: {stats['embedding_index']}")
        
    finally:
        # Cleanup
        kvstore.shutdown()
        shutil.rmtree(temp_dir)
