import pandas as pd
from classifier.TobaccoUserClassifier import TobaccoClassifier
from classifier.preprocessor import Preprocessor
from classifier.va_formatted import VANotesClassification

df = pd.read_excel('data/test.xlsx')
boiler_plate_file = 'res/preprocessor/boiler_plate.json'
snippet_extraction_file = 'res/preprocessor/snippet_extraction.json'
preprocessing_file = 'res/preprocessor/preprocess.json'
VA_questionnaire_extraction_file = 'res/extraction/questionnaire.json'
VA_questionnaire_classification_file = 'res/classification/questionnaire_classification.json'
additional_file = 'res/classification/additional.json'

#preprocess
processed_df = Preprocessor.process_data(df, 'ReportText', 
                                          boiler_plate_file, snippet_extraction_file, preprocessing_file)

#VA FORMAT NOTES
processed_df = VANotesClassification.process_VA_data(processed_df, 'ReportText', 
                                                     VA_questionnaire_extraction_file, VA_questionnaire_classification_file, additional_file)

# tobacco_classifier = TobaccoClassifier('res/preprocesser/preprocess.json', 'res/classification/classify.json', 'res/classification/additional.json','res/extraction/questionnaire.json', 'res/classification/questionnaire_classification.json')

# df = tobacco_classifier.structure_df(df, 'ReportText')


# df = tobacco_classifier.va_questionnaire(df, 'ReportText')

# df = tobacco_classifier.classify_questionnaire(df, 'test')

# df = tobacco_classifier.remove_boiler_plate(df, 'ReportText')
# #
# df = tobacco_classifier.extract_snippet(df, 'ReportText')
# #
# df = tobacco_classifier.preprocess_text(df)

# df = tobacco_classifier.label_status(df)

# df = tobacco_classifier.formatted_va_notes(df, 'snippets')

# df = tobacco_classifier.choose_one(df)
print(processed_df)
processed_df.to_csv('output2.csv')