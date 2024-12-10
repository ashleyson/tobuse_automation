import pandas as pd
import re
from .utils import load_patterns

class Preprocessor:
    @staticmethod
    def process_data(df: pd.DataFrame, text_column: str, 
                     boiler_plate_file: str, snippet_extraction_file: str, preprocessing_file: str) -> pd.DataFrame:
        """
        Compiles boilerplate patterns
        Drops NaNs and coverts to string
        Removes boilerplate text
        Extracts snippets
        Preprocesses extracted snippets
        :param df: DataFrame containing snippets extracted from clinical notes
        :param text_column: Name of the columnn containing clinical notes
        :param boiler_plate_file: Dictionary containing boiler plate patterns
        :param snippet_extraction_file: Dictionary containing snippet extraction patterns
        :param preprocessing_file: Dictionary containing processing patterns 
        :return: DataFrame with preprocessed snippets
        """
        boilerplate_terms = load_patterns(boiler_plate_file)
        snippet_extraction_patterns = load_patterns(snippet_extraction_file)
        preprocessing_patterns = load_patterns(preprocessing_file)
        
        boiler_plate_pattern = re.compile("|".join(boilerplate_terms["boiler_plate_terms"]), re.IGNORECASE)
        snippet_extraction_pattern = re.compile(snippet_extraction_patterns.get("snippet_patterns")[0], re.IGNORECASE)

        df = df.dropna(subset=[text_column])
        df[text_column] = df[text_column].astype('str')

        df = df[~df[text_column].str.contains(boiler_plate_pattern, na=False)]

        df['smoking_snippets'] = df[text_column].apply(
            lambda x: '; '.join(' '.join(match) if isinstance(match, tuple) else match 
                                 for match in snippet_extraction_pattern.findall(x))
        )
        df['smoking_snippets'] = df['smoking_snippets'].astype('str')

        df = Preprocessor.preprocess_text(df, preprocessing_patterns)

        return df

    @staticmethod
    def preprocess_text(df: pd.DataFrame, preprocessing_patterns) -> pd.DataFrame:
        """
        Preprocesses snippets using rule-based patterns
        """
        df['preprocessed_snippets'] = df['smoking_snippets'].astype('str')
        df['preprocessed_snippets'] = df['preprocessed_snippets'].str.lower()
        
        for replacement, patterns in preprocessing_patterns['standardization_patterns1']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True, case=False)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(
                    pattern, replacement, regex=True, flags=re.IGNORECASE, case=False)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'\s+', ' ', x))

        for replacement, patterns in preprocessing_patterns['standardization_patterns2']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(
                    pattern, replacement, regex=True, flags=re.IGNORECASE, case=False)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'[^\w\s]', ' ', x))

        for replacement, patterns in preprocessing_patterns['standardization_patterns3']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(
                    pattern, replacement, regex=True, flags=re.IGNORECASE, case=False)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'\s+', ' ', x))

        return df
