import gzip
import csv
import os
import sys


def unl_gz_to_csv(input_path):
    if not os.path.exists(input_path):
        print(f"Error: Input file '{input_path}' does not exist.")
        return
    base_name = os.path.splitext(os.path.splitext(input_path)[0])[0]
    output_path = f"{base_name}.csv"

    try:
        with gzip.open(input_path, 'rt', encoding='utf-8') as gz_file:
            with open(output_path, 'w', newline='',encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                for line in gz_file:
                    line = line.strip()
                    if line:
                        fields = line.split('\x07')
                        writer.writerow(fields)
        print('Success')
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python unl_gz_to_csv.py <input_file.unl.gz>")
    else:
        unl_gz_to_csv(sys.argv[1])