import pandas as pd 
import re
import json
from .utils import load_patterns
    
class TobaccoClassifier:
    def __init__(self, preprocessing_file, classification_file):
        """
        :param preprocessing_file: Path to the preprocessing patterns file
        :param classification_file: Path to the classification patterns file
        """
        self.preprocessing_patterns = load_patterns(preprocessing_file)
        self.classification_patterns = load_patterns(classification_file)


    def remove_boiler_plate(self, df: pd.DataFrame, text_column: str):
        """
        Removes boiler plate terms from clinical notes

        :param df: DataFrame containing the text data
        :param text_column: Name of the column containing the text data
        :return: DataFrame with boiler plate terms removed
        """
        boiler_plate_pattern = "|".join(self.preprocessing_patterns["boiler_plate_terms"])
        df = df[~df[text_column].str.contains(boiler_plate_pattern, case=False)]
        return df


    def extract_snippet(self, df: pd.DataFrame, text_column: str):
        pattern = self.preprocessing_patterns.get("snippet_patterns")
        df['snippets'] = df[text_column].apply(lambda x: ', '.join(re.findall(pattern, x, re.IGNORECASE)))
        return df
    

    def preprocess_text(self, df: pd.DataFrame, snippets_column: str):
        for standardization_patterns in self.preprocessing_patterns['standardization_patterns1']:
            for replacement, patterns in standardization_patterns.items():
                for pattern in patterns:
                    df['preprocessed_snippets'] = df[snippets_column].apply(lambda x: re.sub(pattern, replacement, x, flags=re.IGNORECASE))
        df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'\s+', ' ', x))
        
        for standardization_patterns in self.preprocessing_patterns['standardization_patterns2']:
            for replacement, patterns in standardization_patterns.items():
                for pattern in patterns:
                    df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(pattern, replacement, x, flags=re.IGNORECASE))
        df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'[^\w\s]', ' ', x))

        for standardization_patterns in self.preprocessing_patterns['standardization_patterns3']:
            for replacement, patterns in standardization_patterns.items():
                for pattern in patterns:
                    df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(pattern, replacement, x, flags=re.IGNORECASE))
        df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'\s+', ' ', x))
        return df

    def label_status(self, df: pd.DataFrame, preprocessed_column: str):
        classification_patterns = self.classification_patterns

        for status, patterns in classification_patterns.items():
            pattern = "|".join(patterns)
            condition = df[preprocessed_column].str.contains(pattern, case=False)

            df.loc[condition,'status'] = status

        df['status'] = df['status'].fillna('UNKNOWN')
    

    #come back to snippet 3