import pandas as pd
from tkinter import Tk, filedialog

# Hide the empty Tkinter window
root = Tk()
root.withdraw()

while True:
    print("\n1. Select CSV")
    print("2. Exit")

    choice = input("Enter choice: ")

    if choice == "1":
        file_path = filedialog.askopenfilename(
            title="Select a CSV file",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )

        # User cancelled the file dialog
        if not file_path:
            print("No file selected.")
            continue

        df = pd.read_csv(file_path)

        print("\nFile:", file_path)
        print("Shape:", df.shape)

        print("\nColumns:")
        print(df.columns.tolist())

        print("\nFirst 5 rows:")
        print(df.head())

        print("\nData types:")
        print(df.dtypes)

        print("\nMissing values:")
        print(df.isnull().sum())

        print("\nStatistics:")
        print(df.describe())

    elif choice == "2":
        print("Exiting...")
        break

    else:
        print("Invalid choice.")

root.destroy()
