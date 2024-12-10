import pandas as pd 
import re
import json
from .utils import load_patterns
class TobaccoClassifier:
    def __init__(self, preprocessing_file, classification_file, additional_file, questionnaire_file, questionnaire_classification_file):
        """
        :param preprocessing_file: Path to the preprocessing patterns file
        :param classification_file: Path to the classification patterns file
        """
        self.preprocessing_patterns = load_patterns(preprocessing_file)
        self.classification_patterns = load_patterns(classification_file)
        self.additional_patterns = load_patterns(additional_file)
        self.questionnaire_patterns = load_patterns(questionnaire_file)
        self.questionnaire_classification_pattern = load_patterns(questionnaire_classification_file)

    def structure_df(self, df: pd.DataFrame, text_column: str):
        df = df.dropna(subset=text_column)
        df[text_column] = df[text_column].astype('str')
        return df

    def remove_boiler_plate(self, df: pd.DataFrame, text_column: str):
        """
        Removes boiler plate terms from clinical notes

        :param df: DataFrame containing the text data
        :param text_column: Name of the column containing the text data
        :return: DataFrame with boiler plate terms removed
        """
        boiler_plate_pattern = "|".join(self.preprocessing_patterns["boiler_plate_terms"])
        df = df[~df[text_column].str.contains(boiler_plate_pattern, case=False, na=False)]
        return df

    def va_questionnaire(self, df: pd.DataFrame, text_column: str):
        tobacco_pattern = self.questionnaire_patterns.get("tobacco_pattern")[0]
        patient_use_pattern = self.questionnaire_patterns.get("patient_use_pattern")[0]
        current_smoker_pattern = self.questionnaire_patterns.get("current_smoker_pattern")[0]
        df['test'] = df[text_column].apply(
            lambda x:
            [match[1].strip() for match in re.findall(tobacco_pattern, x)] +
            [' '.join(filter(None, match)) for match in re.findall(patient_use_pattern, x)] +
            [' '.join(filter(None, match)) for match in re.findall(current_smoker_pattern, x)]
            if re.search(tobacco_pattern, x) or re.search(patient_use_pattern, x) or re.search(current_smoker_pattern, x)
            else 'EMPTY'
        )
        return df

    def classify_questionnaire(self, df: pd.DataFrame, test_column: str):
        def determine_status(test_value):
            if isinstance(test_value, list):
                test_value = ' '.join(test_value)
            if test_value.strip() == "" or test_value=="-":
                return 'NON SMOKER'
            test_value_lower = test_value.lower()

            non_smoker_terms = self.questionnaire_classification_pattern.get("NON SMOKER", [])
            smoker_terms = self.questionnaire_classification_pattern.get("CURRENT SMOKER", [])
            quit_terms = self.questionnaire_classification_pattern.get("QUIT SMOKER", [])
            neg_terms = self.questionnaire_classification_pattern.get("NEG_TERMS", [])

            non_smoker_conditions = any(re.search(r'\b' + re.escape(term) + r'\b', test_value_lower) for term in non_smoker_terms)
            quit_conditions = any(re.search(r'\b' + re.escape(term) + r'\b', test_value_lower) for term in quit_terms) and not any(re.search(r'\b' + re.escape(neg_term) + r'\b', test_value_lower) for neg_term in neg_terms)
            smoker_conditions = any(re.search(r'\b' + re.escape(term) + r'\b', test_value_lower) for term in smoker_terms) and not non_smoker_conditions

            if non_smoker_conditions:
                return 'NON SMOKER'
            elif quit_conditions:
                return 'QUIT SMOKER'
            elif smoker_conditions:
                return 'CURRENT SMOKER'
            else:
                return ''

        df['test_status'] = df[test_column].apply(determine_status)

        return df

    def formatted_va_notes(self, df: pd.DataFrame, text_column: str):
        non_smoker_terms = self.additional_patterns.get("NON SMOKER", [])
        smoker_terms = self.additional_patterns.get("CURRENT SMOKER", [])
        former_smoker_terms = self.additional_patterns.get("FORMER SMOKER", [])
        df['formatted_status'] = df[text_column].apply(
            lambda text: 'NON SMOKER' if any(term in text.lower() for term in non_smoker_terms) else
            ('CURRENT SMOKER' if any(term in text.lower() for term in smoker_terms) else '')
        )
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
            lambda x: '; '.join(' '.join(match) if isinstance(match, tuple)
                                else match for match in re.findall(pattern, x, re.IGNORECASE))
        )
        df['snippets'] = df['snippets'].astype('str')
        return df 
    
    def preprocess_text(self, df: pd.DataFrame):
        """
        Preprocesses snippets using rule-based patterns
        :param df: DataFrame containing snippets extracted from clinical notes
        :return: DataFrame with preprocessed snippets
        """
        df['preprocessed_snippets'] = df['snippets'].astype('str')
        df['preprocessed_snippets'] = df['preprocessed_snippets'].str.lower()
        for replacement, patterns in self.preprocessing_patterns['standardization_patterns1']:
            pattern = "|".join(patterns)
            matched = df['preprocessed_snippets'].str.contains(pattern, regex=True, case=False)
            if matched.any():
                df.loc[matched, 'preprocessed_snippets'] = df.loc[matched, 'preprocessed_snippets'].str.replace(pattern, replacement, regex=True, flags=re.IGNORECASE, case=False)
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

    def classify_status(self, status):
        """
        Classifying snippets into 'former/current/non smoker'
        :param status: Column in df will be classified
        :return: Tobacco Use classification
        """
        classification_patterns = self.classification_patterns
        target_word = "smoke"
        target_pattern = re.compile(r'\b' + re.escape(target_word) + r'\b', re.IGNORECASE)

        best_classification = None
        closest_distance = float('inf')
        matched_keyword = None

        target_match = target_pattern.search(status)

        if target_match:
            target_position = target_match.start()

            for classification, words in classification_patterns.items():
                for word in words:
                    word_pattern = re.compile(word, re.IGNORECASE)
                    word_match = word_pattern.search(status)

                    if word_match:
                        word_position = word_match.start()

                        if word_position < target_position:
                            distance = target_position - (word_position + len(word_match.group()))
                        else:
                            distance = word_position - target_position
                        if distance <= 10:
                            if distance < closest_distance:
                                closest_distance = distance
                                best_classification = classification
                                matched_keyword = word_match.group()
                            elif distance == closest_distance:
                                if word_position < target_position:
                                    best_classification = classification
                                    matched_keyword = word_match.group()

        if best_classification is None:
            for keywords in classification_patterns.get("CURRENT SMOKER", []):
                if re.search(keywords, status, re.IGNORECASE):
                    best_classification = "CURRENT SMOKER"
                    matched_keyword = target_word
                    break

            if best_classification is None:
                best_classification = "UNKNOWN"

        return best_classification, matched_keyword
        
    def label_status(self, df: pd.DataFrame):
        """
        Labeling the preprocessed snippets
        :param df: DataFrame with column to be labeled
        """
        df[['status', 'matched_keyword']] = df['preprocessed_snippets'].apply(lambda x: self.classify_status(x)).apply(pd.Series)
        df['status'] = df['status'].astype('str')
        return df

    def choose_one(self, df: pd.DataFrame):
        df['final_status'] = df.apply(lambda row: row['formatted_status'] if row['formatted_status'] else
                                      row['test_status'] if row['test_status'] else
                                      row['status'], axis=1)
        return df