import pandas as pd
import os
import sys

# Input and output paths
input_file = 'Messages and Media/MemoLabs2/SuperEx_messages.xlsx'
output_dir = os.path.dirname(input_file)
output_file = os.path.join(output_dir, 'SuperEx_messages.csv')

def convert_excel_to_csv():
    try:
        print(f"Python version: {sys.version}")
        print(f"Pandas version: {pd.__version__}")
        print(f"Reading file from: {input_file}")
        print(f"File exists: {os.path.exists(input_file)}")
        
        # Read the Excel file
        df = pd.read_excel(input_file, engine='openpyxl')
        
        # 只保留有用的列
        useful_columns = ['id', 'date', 'type', 'content', 'media_file']
        df = df[useful_columns]
        
        # Save as CSV
        df.to_csv(output_file, index=False)
        print(f"Successfully converted {input_file} to {output_file}")
        
    except Exception as e:
        print(f"Error converting file: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    convert_excel_to_csv()
