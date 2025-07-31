import pandas as pd
import os

# File paths
raw_path = "data/raw/"
processed_path = "data/processed/"
output_file = os.path.join(processed_path, "cleaned_data.csv")

# Create output directory if it doesn't exist
os.makedirs(processed_path, exist_ok=True)

# Load raw files
account_df = pd.read_csv(os.path.join(raw_path, "account.csv"))
customer_df = pd.read_csv(os.path.join(raw_path, "customer.csv"))
transaction_df = pd.read_csv(os.path.join(raw_path, "transaction.csv"))

# Merge transaction with account
merged = transaction_df.merge(account_df, on="account_id", how="left")

# Merge with customer
merged = merged.merge(customer_df, on="customer_id", how="left")

# Basic clean-up: drop nulls in essential columns
merged.dropna(subset=["transaction_id", "amount", "timestamp"], inplace=True)

# Drop PII columns (optional)
columns_to_drop = ["customer_name", "email", "phone_number", "ssn"]
merged.drop(columns=[col for col in columns_to_drop if col in merged.columns], inplace=True)

# Save the cleaned data
merged.to_csv(output_file, index=False)

print("✅ Data cleaned and saved to:", output_file)
