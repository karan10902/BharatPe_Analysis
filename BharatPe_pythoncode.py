# ======= BharatPe Synthetic Data Loading and Exploration =======

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import warnings
warnings.filterwarnings('ignore')

merchants = pd.read_csv(r'C:\Users\karan\Desktop\Bharatpe_Synthetic_data\bharatpe_data\merchants.csv')
enriched_merchants = pd.read_csv(r'C:\Users\karan\Desktop\Bharatpe_Synthetic_data\bharatpe_data\merchants_enriched.csv')
transactions = pd.read_csv(r'C:\Users\karan\Desktop\Bharatpe_Synthetic_data\bharatpe_data\transactions.csv')
interactions = pd.read_csv(r'C:\Users\karan\Desktop\Bharatpe_Synthetic_data\bharatpe_data\interactions.csv')
loans = pd.read_csv(r'C:\Users\karan\Desktop\Bharatpe_Synthetic_data\bharatpe_data\loans.csv')

merchants['onboarding_date'] = pd.to_datetime(merchants['onboarding_date'])
merchants['last_transaction_date'] = pd.to_datetime(merchants['last_transaction_date'])
transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'])
interactions['date'] = pd.to_datetime(interactions['date'])
loans['approval_date'] = pd.to_datetime(loans['approval_date'])
loans['end_date'] = pd.to_datetime(loans['end_date'])

print(f"Total merchants: {merchants.shape[0]}")
print(f"Total transactions: {transactions.shape[0]}")
print(f"Total interactions: {interactions.shape[0]}")
print(f"Total loans: {loans.shape[0]}")

print("\nMissing values in merchants dataset:")
print(merchants.isnull().sum())

# ======= End of BharatPe Data Loading and Exploration =======


# ======= BharatPe Churn Analysis Data Processing =======

import pandas as pd

def churn_analysis(merchants, interactions):
    current_date = merchants['last_transaction_date'].max()
    merchants['days_since_last_txn'] = (current_date - pd.to_datetime(merchants['last_transaction_date'])).dt.days
    merchants['is_churned'] = merchants['days_since_last_txn'] > 30

    churn_rate = merchants['is_churned'].mean()
    print(f"Overall churn rate: {churn_rate:.2%}")

    churn_by_category = merchants.groupby('business_category').agg(
        merchant_count=('merchant_id', 'count'),
        churn_rate=('is_churned', 'mean')
    ).sort_values('churn_rate', ascending=False)

    churn_by_state = merchants.groupby('state').agg(
        merchant_count=('merchant_id', 'count'),
        churn_rate=('is_churned', 'mean')
    ).sort_values('churn_rate', ascending=False)

    churn_by_channel = merchants.groupby('acquisition_channel').agg(
        merchant_count=('merchant_id', 'count'),
        churn_rate=('is_churned', 'mean')
    ).sort_values('churn_rate', ascending=False)

    merchant_interactions = interactions.groupby('merchant_id').agg(
        num_interactions=('interaction_id', 'count'),
        avg_resolution_time=('resolution_time_days', 'mean')
    ).reset_index()

    merchants = merchants.merge(merchant_interactions, on='merchant_id', how='left')
    merchants['num_interactions'].fillna(0, inplace=True)
    merchants['avg_resolution_time'].fillna(0, inplace=True)

    return {
        'overall_churn_rate': churn_rate,
        'churn_by_category': churn_by_category,
        'churn_by_state': churn_by_state,
        'churn_by_channel': churn_by_channel,
        'merchant_interactions': merchant_interactions
    }

# ======= End of BharatPe Churn Analysis Data Processing =======


# ======= BharatPe Product Adoption Analysis Data Processing =======

import os
import pandas as pd

def product_adoption_analysis(merchants, enriched_merchants, output_dir='product_adoption_output'):
    os.makedirs(output_dir, exist_ok=True)

    for col in ['qr_displayed', 'soundbox_adopted', 'swipe_machine']:
        if merchants[col].dtype == object:
            merchants[col] = merchants[col].str.lower().map({'yes': True, 'no': False})
    
    merchants['loan_taken_bool'] = merchants['loans_taken'] > 0

    adoption_rates = {
        'QR Displayed': merchants['qr_displayed'].mean(),
        'Soundbox Adopted': merchants['soundbox_adopted'].mean(),
        'Swipe Machine': merchants['swipe_machine'].mean(),
        'Loans Taken': merchants['loan_taken_bool'].mean()
    }

    print("\n=== Overall Product Adoption Rates ===")
    for product, rate in adoption_rates.items():
        print(f"{product}: {rate:.2%}")
    
    adoption_rates_df = pd.DataFrame(list(adoption_rates.items()), columns=['Product', 'Adoption_Rate'])
    adoption_rates_df.to_csv(os.path.join(output_dir, 'overall_adoption_rates.csv'), index=False)

    adoption_by_category = merchants.groupby('business_category').agg({
        'merchant_id': 'count',
        'qr_displayed': 'mean',
        'soundbox_adopted': 'mean',
        'swipe_machine': 'mean',
        'loan_taken_bool': 'mean'
    }).reset_index()

    adoption_by_category.rename(columns={
        'merchant_id': 'merchant_count',
        'loan_taken_bool': 'loan_adoption_rate'
    }, inplace=True)
    adoption_by_category.to_csv(os.path.join(output_dir, 'adoption_by_category.csv'), index=False)
    
    print("\n=== Adoption by Business Category ===")
    print(adoption_by_category.head())

    merchants['size_category'] = pd.qcut(
        merchants['monthly_transaction_value'],
        4,
        labels=['Small', 'Medium', 'Large', 'Very Large']
    )

    adoption_by_size = merchants.groupby('size_category').agg({
        'merchant_id': 'count',
        'qr_displayed': 'mean',
        'soundbox_adopted': 'mean',
        'swipe_machine': 'mean',
        'loan_taken_bool': 'mean'
    }).reset_index()

    adoption_by_size.rename(columns={
        'merchant_id': 'merchant_count',
        'loan_taken_bool': 'loan_adoption_rate'
    }, inplace=True)
    adoption_by_size.to_csv(os.path.join(output_dir, 'adoption_by_size.csv'), index=False)
    
    print("\n=== Adoption by Merchant Size ===")
    print(adoption_by_size.head())

    adoption_by_state = merchants.groupby('state').agg({
        'merchant_id': 'count',
        'qr_displayed': 'mean',
        'soundbox_adopted': 'mean',
        'swipe_machine': 'mean',
        'loan_taken_bool': 'mean'
    }).reset_index()

    adoption_by_state.rename(columns={
        'merchant_id': 'merchant_count',
        'loan_taken_bool': 'loan_adoption_rate'
    }, inplace=True)
    adoption_by_state.to_csv(os.path.join(output_dir, 'adoption_by_state.csv'), index=False)
    
    print("\n=== Adoption by State (Top 5) ===")
    print(adoption_by_state.head())

    merchants['product_combo'] = merchants.apply(
        lambda x: f"QR: {'Y' if x['qr_displayed'] else 'N'}, "
                  f"Sound: {'Y' if x['soundbox_adopted'] else 'N'}, "
                  f"Swipe: {'Y' if x['swipe_machine'] else 'N'}, "
                  f"Loan: {'Y' if x['loan_taken_bool'] else 'N'}",
        axis=1
    )

    product_combinations = merchants['product_combo'].value_counts().reset_index()
    product_combinations.columns = ['combination', 'count']
    product_combinations['percentage'] = product_combinations['count'] / product_combinations['count'].sum()
    product_combinations.to_csv(os.path.join(output_dir, 'product_combinations.csv'), index=False)
    
    print("\n=== Top 5 Product Combinations ===")
    print(product_combinations.head())

    impact_analysis = merchants.groupby(['qr_displayed', 'soundbox_adopted', 'swipe_machine']).agg({
        'merchant_id': 'count',
        'monthly_transaction_count': 'mean',
        'monthly_transaction_value': 'mean',
        'active_status': lambda x: (x.str.lower() == 'active').mean() if x.dtype == object else (x == 'active').mean()
    }).reset_index()

    impact_analysis.rename(columns={
        'merchant_id': 'merchant_count',
        'active_status': 'active_rate'
    }, inplace=True)
    
    impact_analysis['product_count'] = impact_analysis[['qr_displayed', 'soundbox_adopted', 'swipe_machine']].sum(axis=1)
    impact_analysis.to_csv(os.path.join(output_dir, 'impact_analysis.csv'), index=False)
    
    print("\n=== Impact of Product Adoption on Merchant Metrics ===")
    print(impact_analysis.head())

    print(f"\nAll CSVs saved to folder: {output_dir}\n")

    return {
        'adoption_rates': adoption_rates,
        'adoption_by_category': adoption_by_category,
        'adoption_by_size': adoption_by_size,
        'adoption_by_state': adoption_by_state,
        'product_combinations': product_combinations,
        'impact_analysis': impact_analysis
    }

# ======= End of BharatPe Product Adoption Analysis Data Processing =======


# ======= BharatPe Loan Performance Analysis Data Processing =======

import os
import pandas as pd
import json
from datetime import datetime

def loan_performance_analysis(merchants, loans, transactions, output_dir='output'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"loan_analysis_report_{timestamp}.txt")

    with open(output_file, 'w', encoding='utf-8') as f:
        loan_metrics = {
            'Total Loans': loans.shape[0],
            'Total Loan Amount': float(loans['loan_amount'].sum()),
            'Average Loan Amount': float(loans['loan_amount'].mean()),
            'Default Rate': float((loans['status'] == 'defaulted').mean())
        }
        
        f.write("Overall Loan Metrics:\n")
        for metric, value in loan_metrics.items():
            if 'Rate' in metric:
                f.write(f"{metric}: {value:.2%}\n")
            elif 'Amount' in metric:
                f.write(f"{metric}: ₹{value:,.2f}\n")
            else:
                f.write(f"{metric}: {value:,}\n")
        f.write("\n")
        
        print("Overall Loan Metrics:")
        for metric, value in loan_metrics.items():
            if 'Rate' in metric:
                print(f"{metric}: {value:.2%}")
            elif 'Amount' in metric:
                print(f"{metric}: ₹{value:,.2f}")
            else:
                print(f"{metric}: {value:,}")
        
        merchant_loans = pd.merge(
            loans,
            merchants[['merchant_id', 'business_category', 'state', 'tier', 
                    'acquisition_channel', 'monthly_transaction_value']],
            on='merchant_id',
            how='left'
        )
        
        loan_by_category = merchant_loans.groupby('business_category').agg({
            'loan_id': 'count',
            'loan_amount': 'mean',
            'interest_rate': 'mean',
            'status': lambda x: (x == 'defaulted').mean()
        }).reset_index()
        
        loan_by_category.rename(columns={
            'loan_id': 'loan_count',
            'status': 'default_rate'
        }, inplace=True)
        
        f.write("Loan Performance by Business Category:\n")
        f.write(loan_by_category.to_string(index=False))
        f.write("\n\n")
        
        loan_by_type = merchant_loans.groupby('loan_type').agg({
            'loan_id': 'count',
            'loan_amount': 'mean',
            'interest_rate': 'mean',
            'status': lambda x: (x == 'defaulted').mean()
        }).reset_index()
        
        loan_by_type.rename(columns={
            'loan_id': 'loan_count',
            'status': 'default_rate'
        }, inplace=True)
        
        f.write("Loan Performance by Loan Type:\n")
        f.write(loan_by_type.to_string(index=False))
        f.write("\n\n")
        
        try:
            merchant_loans['txn_volume_category'] = pd.qcut(
                merchant_loans['monthly_transaction_value'],
                4,
                labels=['Low', 'Medium', 'High', 'Very High']
            )
            
            loan_by_txn_volume = merchant_loans.groupby('txn_volume_category').agg({
                'loan_id': 'count',
                'loan_amount': 'mean',
                'interest_rate': 'mean',
                'status': lambda x: (x == 'defaulted').mean()
            }).reset_index()
            
            loan_by_txn_volume.rename(columns={
                'loan_id': 'loan_count',
                'status': 'default_rate'
            }, inplace=True)
            
            f.write("Loan Performance by Transaction Volume:\n")
            f.write(loan_by_txn_volume.to_string(index=False))
            f.write("\n\n")
        except Exception as e:
            f.write(f"Error analyzing transaction volume data: {str(e)}\n\n")
            loan_by_txn_volume = pd.DataFrame()
        
        try:
            merchant_first_loan = loans.groupby('merchant_id').agg({
                'approval_date': 'min'
            }).reset_index()
            
            merchant_first_loan.rename(columns={
                'approval_date': 'first_loan_date'
            }, inplace=True)
            
            transaction_with_loan = pd.merge(
                transactions,
                merchant_first_loan,
                on='merchant_id',
                how='inner'
            )
            
            for date_col in ['transaction_date', 'first_loan_date']:
                if transaction_with_loan[date_col].dtype == 'object':
                    transaction_with_loan[date_col] = pd.to_datetime(
                        transaction_with_loan[date_col], errors='coerce'
                    )
            
            transaction_with_loan['is_after_loan'] = transaction_with_loan['transaction_date'] >= transaction_with_loan['first_loan_date']
            
            merchant_activity_after_loan = transaction_with_loan.groupby(['merchant_id', 'is_after_loan']).agg({
                'transaction_id': 'count',
                'amount': 'sum'
            }).reset_index()
            
            merchant_activity_pivot = merchant_activity_after_loan.pivot_table(
                index='merchant_id',
                columns='is_after_loan',
                values=['transaction_id', 'amount']
            ).reset_index()
            
            merchant_activity_pivot.columns = [
                f"{col[0]}_{col[1]}" if col[1] != "" else col[0] 
                for col in merchant_activity_pivot.columns
            ]
            
            for col in ['transaction_id_False', 'amount_False']:
                if col in merchant_activity_pivot.columns:
                    merchant_activity_pivot[col] = merchant_activity_pivot[col].replace(0, float('nan'))
            
            if all(col in merchant_activity_pivot.columns for col in ['transaction_id_True', 'transaction_id_False']):
                merchant_activity_pivot['txn_count_change'] = merchant_activity_pivot['transaction_id_True'] / merchant_activity_pivot['transaction_id_False'] - 1
            if all(col in merchant_activity_pivot.columns for col in ['amount_True', 'amount_False']):
                merchant_activity_pivot['txn_amount_change'] = merchant_activity_pivot['amount_True'] / merchant_activity_pivot['amount_False'] - 1
            
            f.write("Merchant Activity Before and After Loan:\n")
            f.write(merchant_activity_pivot.head(20).to_string())
            f.write("\n\n")
            
            if 'txn_count_change' in merchant_activity_pivot.columns:
                f.write("Transaction Count Change Summary:\n")
                f.write(merchant_activity_pivot['txn_count_change'].describe().to_string())
                f.write("\n\n")
            
            if 'txn_amount_change' in merchant_activity_pivot.columns:
                f.write("Transaction Amount Change Summary:\n")
                f.write(merchant_activity_pivot['txn_amount_change'].describe().to_string())
                f.write("\n\n")
        except Exception as e:
            f.write(f"Error analyzing merchant activity data: {str(e)}\n\n")
            merchant_activity_pivot = pd.DataFrame()
    
    loan_by_category.to_csv(os.path.join(output_dir, 'loan_by_category.csv'), index=False)
    loan_by_type.to_csv(os.path.join(output_dir, 'loan_by_type.csv'), index=False)
    
    if not loan_by_txn_volume.empty:
        loan_by_txn_volume.to_csv(os.path.join(output_dir, 'loan_by_txn_volume.csv'), index=False)
    
    if not merchant_activity_pivot.empty:
        merchant_activity_pivot.to_csv(os.path.join(output_dir, 'merchant_activity_pivot.csv'), index=False)
    
    print(f"\nAnalysis completed successfully. All results saved to {output_dir} directory.")
    print(f"Main report saved to: {output_file}")
    
    return {
        'loan_metrics': loan_metrics,
        'loan_by_category': loan_by_category,
        'loan_by_type': loan_by_type,
        'loan_by_txn_volume': loan_by_txn_volume,
        'merchant_activity_after_loan': merchant_activity_pivot
    }

def load_csv_files():
    try:
        merchants = pd.read_csv(r'C:\Users\karan\Desktop\Bharatpe_Synthetic_data\bharatpe_data\merchants.csv')
        print(f"Loaded merchants data: {merchants.shape[0]} records")
        
        loans = pd.read_csv(r'C:\Users\karan\Desktop\Bharatpe_Synthetic_data\bharatpe_data\loans.csv')
        print(f"Loaded loans data: {loans.shape[0]} records")
        
        if 'approval_date' in loans.columns:
            loans['approval_date'] = pd.to_datetime(loans['approval_date'], errors='coerce')
        
        transactions = pd.read_csv(r'C:\Users\karan\Desktop\Bharatpe_Synthetic_data\bharatpe_data\transactions.csv')
        print(f"Loaded transactions data: {transactions.shape[0]} records")
        
        if 'transaction_date' in transactions.columns:
            transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'], errors='coerce')
        
        return merchants, loans, transactions
    except Exception as e:
        print(f"Error loading CSV files: {str(e)}")
        raise

if __name__ == "__main__":
    merchants, loans, transactions = load_csv_files()
    loan_analysis = loan_performance_analysis(merchants, loans, transactions)

# ======= End of BharatPe Loan Performance Analysis Data Processing =======


# ======= BharatPe Feature Usage Analysis Data Processing =======

import pandas as pd

def feature_usage_analysis(merchants, transactions):
    payment_method_usage = transactions.groupby('payment_method').agg({
        'transaction_id': 'count',
        'amount': 'sum'
    }).reset_index()
    
    payment_method_usage['txn_share'] = payment_method_usage['transaction_id'] / payment_method_usage['transaction_id'].sum()
    payment_method_usage['amount_share'] = payment_method_usage['amount'] / payment_method_usage['amount'].sum()
    
    transactions['transaction_date_only'] = transactions['transaction_date'].dt.date
    daily_method_usage = transactions.groupby(['transaction_date_only', 'payment_method']).agg({
        'transaction_id': 'count'
    }).reset_index()
    
    daily_method_pivot = daily_method_usage.pivot_table(
        index='transaction_date_only',
        columns='payment_method',
        values='transaction_id',
        fill_value=0
    ).reset_index()
    
    for column in daily_method_pivot.columns:
        if column != 'transaction_date_only':
            daily_method_pivot[f"{column}_7d_ma"] = daily_method_pivot[column].rolling(7).mean()
    
    txn_with_merchant = pd.merge(
        transactions,
        merchants[['merchant_id', 'qr_displayed', 'soundbox_adopted', 'swipe_machine']],
        on='merchant_id',
        how='inner'
    )
    
    merchant_product_usage = txn_with_merchant.groupby('merchant_id').agg({
        'transaction_id': 'count',
        'amount': 'sum',
        'payment_method': lambda x: x.value_counts().index[0],
        'qr_displayed': 'first',
        'soundbox_adopted': 'first',
        'swipe_machine': 'first'
    }).reset_index()
    
    merchant_product_usage['primary_payment_method'] = merchant_product_usage['payment_method']
    merchant_product_usage.drop('payment_method', axis=1, inplace=True)
    
    qr_alignment = merchant_product_usage.groupby(['qr_displayed', 'primary_payment_method']).agg({
        'merchant_id': 'count',
        'transaction_id': 'mean',
        'amount': 'mean'
    }).reset_index()
    
    swipe_alignment = merchant_product_usage.groupby(['swipe_machine', 'primary_payment_method']).agg({
        'merchant_id': 'count',
        'transaction_id': 'mean',
        'amount': 'mean'
    }).reset_index()
    
    merchant_usage_metrics = transactions.groupby('merchant_id').agg({
        'transaction_id': 'count',
        'amount': 'sum',
        'payment_method': lambda x: x.nunique()
    }).reset_index()
    
    merchant_usage_metrics.rename(columns={
        'transaction_id': 'txn_count',
        'amount': 'txn_amount',
        'payment_method': 'payment_methods_used'
    }, inplace=True)
    
    merchant_usage_metrics['usage_frequency'] = pd.qcut(
        merchant_usage_metrics['txn_count'], 
        3, 
        labels=['Low', 'Medium', 'High']
    )
    
    merchant_usage_metrics['usage_diversity'] = pd.cut(
        merchant_usage_metrics['payment_methods_used'],
        bins=[0, 1, 2, 10],
        labels=['Single Method', 'Two Methods', 'Multiple Methods']
    )
    
    merchant_segments = pd.merge(
        merchant_usage_metrics,
        merchants[['merchant_id', 'business_category', 'active_status']],
        on='merchant_id',
        how='inner'
    )
    
    segment_performance = merchant_segments.groupby(['usage_frequency', 'usage_diversity']).agg({
        'merchant_id': 'count',
        'txn_amount': 'mean',
        'active_status': lambda x: (x == 'active').mean()
    }).reset_index()
    
    segment_performance.rename(columns={
        'merchant_id': 'merchant_count',
        'active_status': 'active_rate'
    }, inplace=True)
    
    return {
        'payment_method_usage': payment_method_usage,
        'daily_method_usage': daily_method_pivot,
        'qr_alignment': qr_alignment,
        'swipe_alignment': swipe_alignment,
        'segment_performance': segment_performance
    }

# ======= End of BharatPe Feature Usage Analysis Data Processing =======
