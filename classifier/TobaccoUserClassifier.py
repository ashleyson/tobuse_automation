import pandas as pd 
import re
import json
from .utils import load_patterns


class TobaccoClassifier:
    def __init__(self, preprocessing_file, classification_file, additional_file):
        """
        :param preprocessing_file: Path to the preprocessing patterns file
        :param classification_file: Path to the classification patterns file
        """
        self.preprocessing_patterns = load_patterns(preprocessing_file)
        self.classification_patterns = load_patterns(classification_file)
        self.additional_patterns = load_patterns(additional_file)


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
        """
        Extracts snippets from clinical notes
        :param df: DataFrame containing clincal notes
        :param text_column: Name of column containing text data
        :return: DataFrame with extracted snippets
        """
        pattern = self.preprocessing_patterns.get("snippet_patterns")[0] 
        df['snippets'] = df[text_column].apply(
            lambda x: '; '.join(' '.join(match) if isinstance(match, tuple) else match for match in re.findall(pattern, x, re.IGNORECASE))
        )
        return df 
    
    def preprocess_text(self, df: pd.DataFrame):
        """
        Preprocesses snippets using rule-based patterns
        :param df: DataFrame containing snippets extracted from clinical notes
        :return: DataFrame with preprocessed snippets
        """
        df['preprocessed_snippets'] = df['snippets']
        
        for replacement, patterns in self.preprocessing_patterns['standardization_patterns1']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'\s+', ' ', x))
            
        for replacement, patterns in self.preprocessing_patterns['standardization_patterns2']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'[^\w\s]', ' ', x))

        for replacement, patterns in self.preprocessing_patterns['standardization_patterns3']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE)
            df['preprocessed_snippets'] = df['preprocessed_snippets'].apply(lambda x: re.sub(r'\s+', ' ', x))
            
        return df

    def classify_status(self, status):
        """
        Classifying snippets into 'former/current/non smoker'
        :param status: Column in df will be classified
        :return: Tobacco Use classification
        """
        classification_patterns = self.classification_patterns
        for classification, words in classification_patterns.items():
            for word in words:
                if re.search(word, status, re.IGNORECASE):
                    return classification
        return None  
        
    def label_status(self, df: pd.DataFrame):
        """
        Labeling the preprocessed snippets
        :param df: DataFrame with column to be labeled
        """
        df['status'] = df['preprocessed_snippets'].apply(self.classify_status)
        return df
    
    def label_additional_status(self, df: pd.DataFrame):
        """
        Extracting an additional snippet from clinical notes 
        :param df: DataFrame with second snippet 
        """
        additional_patterns = self.additional_patterns
        df['snippet2'] = df['snippets']

        for replacement, patterns in self.additional_patterns['additional_patterns']:
            pattern = "|".join(patterns)
            matched = df['snippet2'].str.contains(pattern, regex=True)
            if matched.any():
                df.loc[matched, 'snippet2'] = df.loc[matched, 'snippet2'].str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE)
        return df
    
    def extract_status2(self, df: pd.DataFrame):
        patterns = self.additional_patterns.get("pattern_for_extraction")
        df['snippet2'] = df['snippet2'].apply(lambda x: re.sub(r'\s+',' ',x))
        df['status2'] = df['snippet2'].apply(
            lambda x: '; '.join(', '.join(re.findall(pattern, x, re.IGNORECASE)) if isinstance(match, tuple) 
                                else match for pattern in patterns for match in re.findall(pattern, x, re.IGNORECASE))
        )
        
        df['status3'] = df['status2'].apply(self.classify_status)
        return df
        

