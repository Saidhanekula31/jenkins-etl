import pandas as pd
import os

# Input paths
raw_path = "../data/raw"
output_path = "../data/processed/cleaned_transactions.csv"

# Load data
account_df = pd.read_csv(f"{raw_path}/account.csv")
customer_df = pd.read_csv(f"{raw_path}/customer.csv")
transaction_df = pd.read_csv(f"{raw_path}/transaction.csv")

# Remove PII from customer
customer_cleaned = customer_df.drop(columns=["email", "ssn_plain", "dob"])

# Join: transaction -> account -> customer
merged = transaction_df.merge(account_df, on="account_id", how="left") \
                       .merge(customer_cleaned, on="customer_id", how="left")

# Convert amount_cents to dollars
merged["amount_dollars"] = merged["amount_cents"] / 100.0

# Filter only 'posted' transactions
merged_cleaned = merged[merged["status"] == "posted"]

# Drop unnecessary columns (handle possible name conflicts)
columns_to_drop = [col for col in merged_cleaned.columns if "amount_cents" in col or "created_at" in col or "updated_at" in col or "status" in col]
merged_cleaned = merged_cleaned.drop(columns=columns_to_drop)

# Save output
os.makedirs("../data/processed", exist_ok=True)
merged_cleaned.to_csv(output_path, index=False)
print(f"✅ Saved cleaned data to: {output_path}")