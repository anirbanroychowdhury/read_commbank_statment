import tabula
import pandas as pd
import numpy as np
import os

# read PDF file
pdfDoc = tabula.read_pdf("TransactionSummary.pdf", pages="all", pandas_options={'header': None})

correctHeaderFormatPdfDoc = []
finalDf = []

# #Correct problamatic headers
for df in pdfDoc:
    for index, row in df.iterrows():
        if 'Date' in row.values:
            df.columns = row.values
            df = df.iloc[index+1:]
            df = df.reset_index(drop=True)
            correctHeaderFormatPdfDoc.append(df)
            break

# ETL for the data
for df in correctHeaderFormatPdfDoc:
    df.rename(columns={'Date': 'date', 'Transaction details': 'transaction_details', 'Amount':'amount', 'Balance':'balance'}, inplace=True)
    #indices to drop at the end
    indices_to_drop = []
    #iterate over df
    for i, row in df.iterrows():
        #count of nans in a row
        nans = row.isna().sum()
        #if nans value are greater than 3. i.e majority columns
        if nans >= 3:
            #drop all non nan values
            non_nans = row.dropna().tolist()
            if i > 0:
                #join it to the column value of a row above
                df.loc[i-1, 'transaction_details'] = df.loc[i-1, 'transaction_details'] + ''.join(non_nans)
            #drop all 
            indices_to_drop.append(i)
    df.drop(indices_to_drop, inplace=True)
    df = df.reset_index(drop=True)
    finalDf.append(df)

# Remove Majority null values columns.
for df in finalDf:
    df = df.drop(columns=[np.nan], errors='ignore', inplace=True)

#combine the list of pd into one single pd
combined_df = pd.concat(finalDf, ignore_index=True)
combined_df = combined_df.reindex(columns=['date', 'amount', 'transaction_details', 'balance'])
combined_df['amount'] = combined_df['amount'].str.replace('$','')
combined_df['amount'] = combined_df['amount'].str.replace(',','')
combined_df['amount'] = combined_df['amount'].astype(float)
income = combined_df[combined_df['amount']>0]
expense = combined_df[combined_df['amount']<0]
expense['amount'] = expense['amount'].astype(str).str.replace('-','')

# Create a directory to store the CSV files
directory = 'processed_data/'+pd.to_datetime(combined_df['date'].iloc[0]).strftime('%Y%m')
if not os.path.exists(directory):
    os.makedirs(directory)
income.to_csv(os.path.join(directory, 'income.csv'), index=False)
expense.to_csv(os.path.join(directory, 'expense.csv'), index=False)

