import pandas as pd 
import re
import json
from .utils import load_patterns

class TobaccoClassifier:
    def __init__(self, classification_file):  
        self.classification_patterns = load_patterns(classification_file)

    def classify_and_label(self, df: pd.DataFrame, text_column: str) -> pd.DataFrame:
        """
        Classifies snippets in the specified text column of the DataFrame into 
        'former/current/non smoker' and labels them.
        
        :param df: DataFrame with the column to be classified
        :param text_column: Name of the column containing snippets to classify
        :return: DataFrame with added classification status and matched keyword
        """
        # Add new columns for status and matched keyword
        df['status'] = None
        df['matched_keyword'] = None
        
        target_word = "smoke"
        target_pattern = re.compile(r'\b' + re.escape(target_word) + r'\b', re.IGNORECASE)

        for i, row in df.iterrows():
            snippet_text = row[text_column]
            target_match = target_pattern.search(snippet_text)
            best_classification = None
            closest_distance = float('inf')
            matched_keyword = None
            
            if target_match:
                target_position = target_match.start()
                
                for classification, words in self.classification_patterns.items():
                    for word in words:
                        word_pattern = re.compile(word, re.IGNORECASE)
                        word_match = word_pattern.search(snippet_text)
                        
                        if word_match:
                            word_position = word_match.start()
                            distance = abs(word_position - target_position)
                            if distance <= 10:
                                if distance < closest_distance:
                                    closest_distance = distance
                                    best_classification = classification
                                    matched_keyword = word_match.group()
                                elif distance == closest_distance and word_position < target_position:
                                    best_classification = classification
                                    matched_keyword = word_match.group()

            if best_classification is None:
                for keywords in self.classification_patterns.get("CURRENT SMOKER", []):
                    if re.search(keywords, snippet_text, re.IGNORECASE):
                        best_classification = "CURRENT SMOKER"
                        matched_keyword = target_word
                        break
            
            if best_classification is None:
                best_classification = "UNKNOWN"
                
            df.at[i, 'status'] = best_classification
            df.at[i, 'matched_keyword'] = matched_keyword

        # Ensure the status is typed as a string
        df['status'] = df['status'].astype('str')
        
        return df
