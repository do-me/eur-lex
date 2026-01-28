import polars as pl

# optional preprocessing logic here
# kept intentionally lean as overprocessing is detrimental for multi-purpose downstream tasks, see e.g. https://huggingface.co/datasets/EuropeanParliament/Eurovoc/discussions/5
def clean_text_batch(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("text")
        # 1. Fix space before period: "word ." -> "word."
        #.str.replace_all(r"\s+\.", ".")
        
        # 2. Fix missing space after period: "word.Next" -> "word. Next"
        # Only if the period is followed by a Capital Letter (common sentence pattern)
        #.str.replace_all(r"\.([A-Z])", r". $1")
        
        # 3. Collapse multiple spaces into one
        #.str.replace_all(r" {2,}", " ")
        
        # 4. Handle "Dirty PDF" specific: Hyphenation at line breaks
        # "pre- processing" -> "preprocessing"
        #.str.replace_all(r"(\w)-\s+(\w)", r"$1$2")
        
        # 5. Optional: Fix common PDF ligatures if your parser didn't
        #.str.replace_all("ﬁ", "fi").str.replace_all("ﬂ", "fl")
        
        #.str.strip_chars()
    )

def match_keywords(df: pl.DataFrame, keywords: list[str]) -> pl.DataFrame:
    """Add boolean column for each keyword found in 'text'."""
    if not keywords:
        return df
        
    expressions = []
    for kw in keywords:
        # Create a safe column name
        col_name = f"match_{kw.lower().replace(' ', '_').replace('-', '_')}"
        # Case-insensitive literal search using regex (?i)
        # We escape the keyword to ensure special characters don't break regex
        import re
        pattern = f"(?i){re.escape(kw)}"
        expressions.append(
            pl.col("text").str.contains(pattern).alias(col_name)
        )
    
    return df.with_columns(expressions)

def filter_keyword_matches(df: pl.DataFrame, keywords: list[str]) -> pl.DataFrame:
    """Filter rows that have at least one keyword match."""
    if not keywords or df.is_empty():
        return df
        
    match_cols = [f"match_{kw.lower().replace(' ', '_').replace('-', '_')}" for kw in keywords]
    return df.filter(pl.any_horizontal(pl.col(match_cols)))