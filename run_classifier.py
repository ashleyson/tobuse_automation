import pandas as pd
from classifier.TobaccoUserClassifier import TobaccoClassifier

df = pd.read_excel('data/test.xlsx')

tobacco_classifier = TobaccoClassifier('res/preprocess.json', 'res/classify.json', 'res/additional.json')

df = tobacco_classifier.remove_boiler_plate(df, 'text')

df = tobacco_classifier.extract_snippet(df, 'text')

df = tobacco_classifier.preprocess_text(df)

df = tobacco_classifier.label_status(df)

df = tobacco_classifier.label_additional_status(df)

df = tobacco_classifier.extract_status2(df)
print(df)
df.to_csv('output.csv')


