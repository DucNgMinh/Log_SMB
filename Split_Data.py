import pandas as pd
import os
from datetime import timedelta

def split_data_by_custom_day_to_parquet(file_path, output_dir):
    # Load the data
    data = pd.read_csv(file_path)
    data['Request'] = data['Request'].str.split(' -').str[0]
    # Convert 'Ngày yêu cầu' to datetime
    data['Ngày yêu cầu'] = pd.to_datetime(data['Ngày yêu cầu'], format='%d/%m/%Y %I:%M:%S %p')
    data['Ngày phản hồi'] = pd.to_datetime(data['Ngày phản hồi'], format='%d/%m/%Y %I:%M:%S %p')

    # Adjust the datetime by subtracting 5 hours
    data['Adjusted Date'] = data['Ngày yêu cầu'] - timedelta(hours=5)

    # Group by the date of the adjusted datetime
    grouped = data.groupby(data['Adjusted Date'].dt.date)

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Save each group to a Parquet file
    for date, group in grouped:
        # Remove the 'Adjusted Date' column before saving
        group = group.drop(columns=['Adjusted Date'])

        # Format the date as YYYYMMDD for the file name
        formatted_date = date.strftime('%Y%m%d')

        file_name = f'{formatted_date}.parquet'
        file_path = os.path.join(output_dir, file_name)
        group.to_parquet(file_path, index=False)
        print(f'Saved: {file_name}')

if __name__ == "__main__":
    input_file = r'C:\Users\Admin\PycharmProjects\Log SmartBanking\Log_Data.csv'  # Replace with your file path
    output_directory = r'C:\Users\Admin\PycharmProjects\Log SmartBanking\Log'  # Replace with your desired output directory

    split_data_by_custom_day_to_parquet(input_file, output_directory)