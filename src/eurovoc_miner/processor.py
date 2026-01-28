import polars as pl

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