import pandas as pd
import re
from .utils import load_patterns

class Preprocessor:
    def __init__(self, boiler_plate_file: str, snippet_extraction_file: str, preprocessing_file: str):
        """
        Load patterns during initialization.
        :param boiler_plate_file: Path to boilerplate patterns
        :param snippet_extraction_file: Path to snippet extraction patterns
        :param preprocessing_file: Path to preprocessing patterns
        """
        self.boilerplate_terms = load_patterns(boiler_plate_file)
        self.snippet_extraction_patterns = load_patterns(snippet_extraction_file)
        self.preprocessing_patterns = load_patterns(preprocessing_file)

        self.boiler_plate_pattern = re.compile("|".join(self.boilerplate_terms["boiler_plate_terms"]), re.IGNORECASE)
        self.snippet_extraction_pattern = re.compile(self.snippet_extraction_patterns.get("snippet_patterns")[0], re.IGNORECASE)

    def process_data(self, df: pd.DataFrame, text_column: str) -> pd.DataFrame:
        """
        Processes the DataFrame to extract and preprocess smoking snippets.
        :param df: DataFrame containing snippets extracted from clinical notes
        :param text_column: Name of the column containing clinical notes
        :return: DataFrame with processed snippets
        """
        df = df.dropna(subset=[text_column])
        df[text_column] = df[text_column].astype('str')

        df = df[~df[text_column].str.contains(self.boiler_plate_pattern, na=False)]

        df['smoking_snippets'] = df[text_column].apply(
            lambda x: '; '.join(' '.join(match) if isinstance(match, tuple) else match 
                                 for match in self.snippet_extraction_pattern.findall(x))
        )
        df['smoking_snippets'] = df['smoking_snippets'].astype('str')

        df = self.preprocess_text(df)

        return df

    def preprocess_text(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess extracted snippets using rule-based patterns.
        :param df: DataFrame containing smoking snippets
        :return: DataFrame with preprocessed snippets
        """
        df['preprocessed_snippets'] = df['smoking_snippets'].astype('str')
        
        for replacement, patterns in self.preprocessing_patterns['standardization_patterns1']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True, case=False)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'\s+', ' ', x))

        for replacement, patterns in self.preprocessing_patterns['standardization_patterns2']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE, case=False)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'[^\w\s]', ' ', x))

        for replacement, patterns in self.preprocessing_patterns['standardization_patterns3']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE, case=False)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'\s+', ' ', x))

        return df
