import json 

def load_patterns(file_path):
    """
    Load patterns from JSON file
    """
    with open(file_path, 'r') as file:
        return json.load(file)

