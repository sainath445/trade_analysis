import pandas as pd
import json

# Load the dataset
file_path = 'TRADES_CopyTr_90D_ROI.csv'  # Update the path if needed
df = pd.read_csv(file_path)

# Step 1: Inspect the dataset columns
print("Initial columns in the dataframe:", df.columns.tolist())

# Log some raw entries from the Trade_History column for inspection
print("Sample raw Trade_History entries:")
print(df['Trade_History'].head(5).to_string(index=False))  # Display the first 5 entries

# Step 2: Clean the Trade_History column
def clean_trade_history(text):
    if isinstance(text, str):  # Check if the entry is a string
        # Replace single quotes with double quotes
        text = text.replace("'", '"')
        # Remove any trailing commas before closing braces
        text = text.replace(",}", "}").replace(",]", "]")
        return text
    return None  # Return None for non-string entries (e.g., NaN)

# Apply the cleanup function
df['Trade_History'] = df['Trade_History'].apply(clean_trade_history)

# Log cleaned entries to see if the cleanup worked
print("Sample cleaned Trade_History entries:")
print(df['Trade_History'].head(5).to_string(index=False))  # Display the first 5 cleaned entries

# Step 3: Parse the JSON in Trade_History
def safe_json_parse(text):
    if text is None:  # Check for None values
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e} for text: {text}")  # Log the error for review
        return None  # Return None if parsing fails

# Apply the safe parsing function
df['parsed_trade_history'] = df['Trade_History'].apply(safe_json_parse)

# Step 4: Normalize the parsed trade history into a DataFrame
trade_history_df = pd.json_normalize(df['parsed_trade_history'])

# Check if trade_history_df is empty
if trade_history_df.empty:
    print("Parsed Trade History DataFrame is empty. Please check the data cleaning and parsing steps.")
else:
    # Step 5: Define required columns
    required_columns = ['Port_IDs', 'side', 'positionSide', 'price', 'quantity', 'qty', 'realizedProfit', 'timestamp']
    existing_columns = [col for col in required_columns if col in trade_history_df.columns]
    
    # Filter only the required columns
    trade_history_df = trade_history_df[existing_columns]

    # Step 6: Calculate financial metrics
    def calculate_metrics(df):
        metrics = {
            'ROI': (df['realizedProfit'].sum() / df['quantity'].sum()) * 100 if df['quantity'].sum() != 0 else 0,
            'PnL': df['realizedProfit'].sum(),
            'Total Positions': len(df),
            'Win Positions': (df['realizedProfit'] > 0).sum(),
        }
        metrics['Win Rate'] = metrics['Win Positions'] / metrics['Total Positions'] * 100 if metrics['Total Positions'] > 0 else 0
        return metrics

    # Group by Port_IDs and calculate metrics
    account_metrics = trade_history_df.groupby('Port_IDs').apply(calculate_metrics).reset_index()

    # Step 7: Rank accounts based on metrics and get top 20
    account_metrics['Rank'] = account_metrics['ROI'].rank(ascending=False)  # Example ranking by ROI
    top_20_accounts = account_metrics.nlargest(20, 'ROI')

    # Display the top 20 accounts
    print("Top 20 accounts based on ROI:")
    print(top_20_accounts)

    # Step 8: Save the results to CSV
    account_metrics.to_csv('account_metrics.csv', index=False)
    top_20_accounts.to_csv('top_20_accounts.csv', index=False)

    print("Analysis completed and results saved to CSV files.")
