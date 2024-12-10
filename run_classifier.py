import pandas as pd
from classifier.TobaccoUserClassifier import TobaccoClassifier

df = pd.read_excel('data/test.xlsx')

tobacco_classifier = TobaccoClassifier('res/preprocess.json', 'res/classify.json', 'res/additional.json','res/questionnaire.json', 'res/questionnaire_classification.json')

df = tobacco_classifier.structure_df(df, 'ReportText')


df = tobacco_classifier.va_questionnaire(df, 'ReportText')

df = tobacco_classifier.classify_questionnaire(df, 'test')

df = tobacco_classifier.remove_boiler_plate(df, 'ReportText')
#
df = tobacco_classifier.extract_snippet(df, 'ReportText')
#
df = tobacco_classifier.preprocess_text(df)

df = tobacco_classifier.label_status(df)

df = tobacco_classifier.formatted_va_notes(df, 'snippets')

df = tobacco_classifier.choose_one(df)
print(df)
df.to_csv('output2.csv')