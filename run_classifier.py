import pandas as pd
from classifier.TobaccoUserClassifier import TobaccoClassifier
from classifier.preprocessor import Preprocessor
from classifier.va_formatted import VANotesClassification
from classifier.postprocessor import Postprocessor

df = pd.read_excel('data/test.xlsx')
boiler_plate_file = 'res/preprocessor/boiler_plate.json'
snippet_extraction_file = 'res/preprocessor/snippet_extraction.json'
preprocessing_file = 'res/preprocessor/preprocess.json'
VA_questionnaire_extraction_file = 'res/va_formatted/VA_questionnaire_extraction.json'
VA_questionnaire_classification_file = 'res/va_formatted/VA_questionnaire_classification.json'
additional_VA_patterns_file = 'res/va_formatted/additional_VA_patterns.json'
classification_file = 'res/classification/classify.json'


#preprocessor
preprocessor = Preprocessor(boiler_plate_file, snippet_extraction_file, preprocessing_file)
processed_df = preprocessor.process_data(df, 'ReportText')

#VA FORMAT NOTES
vanotesclassification = VANotesClassification(VA_questionnaire_extraction_file, VA_questionnaire_classification_file, additional_VA_patterns_file)
processed_df = vanotesclassification.process_VA_data(processed_df, 'ReportText')

#classification
classification = TobaccoClassifier(classification_file)
result_df = classification.classify_and_label(processed_df, 'preprocessed_snippets')

#postprocessor
result_df =  Postprocessor.choose_one(result_df)

print(list(result_df))
print(result_df)
result_df.to_csv('output2.csv')