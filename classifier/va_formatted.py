import pandas as pd
import re
import json
from .utils import load_patterns
class VANotesClassification:
    @staticmethod
    def process_VA_data(df: pd.DataFrame, text_column: str, 
                        VA_questionnaire_extraction_file: str, VA_questionnaire_classification_file: str, additional_file: str) -> pd.DataFrame:
        """
        Load extraction and classification patterns
        Extract relevant patterns
        Extract matches into a new column
        Define classifications for the extracted data
        Classify status 
        :param df: DataFrame containing snippets extracted from clinical notes
        :param text_column: Name of the column containing clinical notes
        :param VA_questionnaire_extraction_file: Dictionary containing VA questionnaire extraction patterns
        :param VA_questionnaire_classification_file: Dictionary containing VA questionnaire classification patterns
        :param additional_file: Dictionary containing additional VA formatted patterns
        :return: DataFrame with classified VA formatted notes
        """

        VA_questionnaire_extraction_terms = load_patterns(VA_questionnaire_extraction_file)
        VA_questionnaire_classification_terms = load_patterns(VA_questionnaire_classification_file)
        VA_additional_terms = load_patterns(additional_file)

        VA_questionnaire_tobacco_pattern = VA_questionnaire_extraction_terms.get("tobacco_pattern")[0]
        VA_questionnaire_patient_use_pattern = VA_questionnaire_extraction_terms.get("patient_use_pattern")[0]
        VA_questionnaire_current_smoker_pattern = VA_questionnaire_extraction_terms.get("current_smoker_pattern")[0]

        df['VA_formatted_extraction'] = df[text_column].apply(
            lambda x: (
                [match[1].strip() for match in re.findall(VA_questionnaire_tobacco_pattern, x)] +
                [' '.join(filter(None, match)) for match in re.findall(VA_questionnaire_patient_use_pattern, x)] +
                [' '.join(filter(None, match)) for match in re.findall(VA_questionnaire_current_smoker_pattern, x)]
                if re.search(VA_questionnaire_tobacco_pattern, x) or re.search(VA_questionnaire_patient_use_pattern, x) or re.search(VA_questionnaire_current_smoker_pattern, x)
                else ['EMPTY']
            )
        )

        VA_questionnaire_non_smoker_terms = VA_questionnaire_classification_terms.get("NON SMOKER", [])
        VA_questionnaire_smoker_terms = VA_questionnaire_classification_terms.get("CURRENT SMOKER", [])
        VA_questionnaire_quit_terms = VA_questionnaire_classification_terms.get("QUIT SMOKER", [])
        VA_questionnaire_neg_terms = VA_questionnaire_classification_terms.get("NEG_TERMS", [])

        df['VA_formatted_classification'] = df['VA_formatted_extraction'].apply(
            lambda test_value: (
                'NON SMOKER' if any(re.search(r'\b' + re.escape(term) + r'\b', ' '.join(test_value).lower()) for term in VA_questionnaire_non_smoker_terms) else
                'QUITTER SMOKER' if any(re.search(r'\b' + re.escape(term) + r'\b', ' '.join(test_value).lower()) for term in VA_questionnaire_quit_terms) and 
                                      not any(re.search(r'\b' + re.escape(neg_term) + r'\b', ' '.join(test_value).lower()) for neg_term in VA_questionnaire_neg_terms) else
                'CURRENT SMOKER' if any(re.search(r'\b' + re.escape(term) + r'\b', ' '.join(test_value).lower()) for term in VA_questionnaire_smoker_terms) else
                ''
            )
        )

        VA_additional_non_smoker_terms = VA_additional_terms.get("NON SMOKER", [])
        VA_additional_smoker_terms = VA_additional_terms.get("CURRENT SMOKER", [])
        VA_additional_former_smoker_terms = VA_additional_terms.get("FORMER SMOKER", [])

        df['formatted_status'] = df[text_column].apply(
            lambda text: (
                'NON SMOKER' if any(term in text.lower() for term in VA_additional_non_smoker_terms) else
                'FORMER SMOKER' if any(term in text.lower() for term in VA_additional_former_smoker_terms) else
                'CURRENT SMOKER' if any(term in text.lower() for term in VA_additional_smoker_terms) else
                ''
            )
        )
        
        return df