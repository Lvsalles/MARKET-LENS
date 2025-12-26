def apply_semantic_dictionary(df, dictionary):
    df = df.copy()
    lower_cols = {c.lower(): c for c in df.columns}

    rename_map = {}

    for canonical, aliases in dictionary.items():
        for alias in aliases:
            if alias in lower_cols:
                rename_map[lower_cols[alias]] = canonical

    df = df.rename(columns=rename_map)
    return df
