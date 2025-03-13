import pandas as pd
import argparse
import os
from classifier.TobaccoUserClassifier import TobaccoClassifier
from classifier.preprocessor import Preprocessor
from classifier.va_formatted import VANotesClassification
from classifier.postprocessor import Postprocessor

def main(input_file, output_file, text_column):
    # File paths for the resources
    boiler_plate_file = 'res/preprocessor/boiler_plate.json'
    snippet_extraction_file = 'res/preprocessor/snippet_extraction.json'
    preprocessing_file = 'res/preprocessor/preprocess.json'
    VA_questionnaire_extraction_file = 'res/va_formatted/VA_questionnaire_extraction.json'
    VA_questionnaire_classification_file = 'res/va_formatted/VA_questionnaire_classification.json'
    additional_VA_patterns_file = 'res/va_formatted/additional_VA_patterns.json'
    current_user_file = 'res/concept_tagger/current_user.json'
    former_user_file = 'res/concept_tagger/former_user.json'
    quit_user_file = 'res/concept_tagger/quit_user.json'
    non_user_file = 'res/concept_tagger/non_user.json'

    # Check file extension
    file_extension = os.path.splitext(input_file)[1].lower()

    if file_extension == '.csv':
        try:
            df = pd.read_csv(input_file, encoding='ISO-8859-1') 
        except UnicodeDecodeError:
            print(f"Failed to decode the file {input_file}. Please check the file encoding.")
            return
    elif file_extension in ['.xls', '.xlsx']:
        print("You provided an Excel file. Please either convert it to CSV or modify the script to use 'pd.read_excel'.")
        return
    else:
        print("Unsupported file type. Please provide a CSV or Excel file.")
        return  

    # Check if the specified text column exists
    if text_column not in df.columns:
        print(f"Error: The specified text column '{text_column}' was not found in the input file.")
        print(f"Available columns: {list(df.columns)}")
        return

    # Preprocessing
    preprocessor = Preprocessor(boiler_plate_file, snippet_extraction_file, preprocessing_file)
    processed_df = preprocessor.process_data(df, text_column)

    # VA Formatted Notes
    vanotesclassification = VANotesClassification(VA_questionnaire_extraction_file, VA_questionnaire_classification_file, additional_VA_patterns_file)
    processed_df = vanotesclassification.process_VA_data(processed_df, text_column)

    # Classification
    classification = TobaccoClassifier(current_user_file, former_user_file, quit_user_file, non_user_file)
    result_df = classification.classify_and_label(processed_df, 'preprocessed_snippets')

    # Postprocessing
    result_df = Postprocessor.choose_one(result_df)

    if 'final_status' in result_df.columns:
        output_df = df.copy()  
        output_df['matched_keyword'] = result_df['matched_keyword']  # Uncomment this line if you also want to include the matched_keyword column in the output
        output_df['final_status'] = result_df['final_status']  
        

    else:
        print("Error: 'final_status' column not found in result. Ensure classification ran correctly.")
        return

    # Save to output CSV
    output_df.to_csv(output_file, index=False)

    print(f"Results saved to {output_file}")
    print("Columns in output file:", list(output_df.columns))
    print(output_df.head()) 


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run tobacco use detection on clinical data.')
    parser.add_argument('--input', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('--output', type=str, required=True, help='Path to save the output CSV file')
    parser.add_argument('--text_column', type=str, required=True, help='Name of the column containing text data')

    args = parser.parse_args()
    main(args.input, args.output, args.text_column)
