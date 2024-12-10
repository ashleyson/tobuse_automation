import pandas as pd

class Postprocessor:
    def choose_one(df: pd.DataFrame) -> pd.DataFrame:
        """
        Choose the final status based on the priority of status columns.
        :param df: DataFrame containing the status columns to be processed.
        :return: DataFrame with a new column 'final_status'.
        """
        df['final_status'] = df.apply(lambda row: row['formatted_status'] if row['formatted_status'] else
                                       row['VA_formatted_classification'] if row['VA_formatted_classification'] else
                                       row['status'], axis=1)
        return df