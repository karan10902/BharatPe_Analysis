# BharatPe Google Play Store Reviews Scraper
# Fetches 5000 latest reviews and exports to CSV/Excel

from google_play_scraper import reviews, Sort
import pandas as pd

all_reviews = []
token = None

for i in range(5):
    result, token = reviews(
        'com.bharatpe.app',
        lang='en',
        country='in',
        sort=Sort.NEWEST,
        count=5000,
        continuation_token=token
    )
    all_reviews.extend(result)
    print(f'Fetched {len(all_reviews)} reviews so far...')

df = pd.DataFrame(all_reviews)
df.to_csv('bharatpe_reviews.csv', index=False)
df.to_excel('bharatpe_reviews.xlsx', index=False)




# =======Simple web scraper to fetch and print the title of BharatPe's homepage using requests and BeautifulSoup.=======


import requests
from bs4 import BeautifulSoup

url = 'https://bharatpe.com/'

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers)


soup = BeautifulSoup(response.text, 'html.parser')

print(soup.title.text)



# =======BharatPe Synthetic Dataset Generator =======

Generates realistic synthetic merchant profiles, transaction histories, 
customer interactions, and loan data for BharatPe merchants.

Outputs CSV files for merchants, transactions, interactions, and loans.

Designed for testing, analysis, and demos without real data.

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import names
import os

np.random.seed(42)
random.seed(42)

BUSINESS_CATEGORIES = {
    'Retail': ['General Store', 'Grocery', 'Clothing', 'Electronics', 'Hardware', 'Mobile Shop', 'Medical Store', 'Stationery'],
    'Food & Beverage': ['Restaurant', 'Cafe', 'Sweet Shop', 'Bakery', 'Tea Stall', 'Juice Center', 'Dhaba', 'Fast Food'],
    'Services': ['Salon', 'Tailor', 'Laundry', 'Mobile Repair', 'Electronics Repair', 'Printing Services', 'Travel Agency'],
    'Wholesale': ['Grocery Wholesale', 'Textile Wholesale', 'Electronics Wholesale', 'Stationery Wholesale'],
}

ACQUISITION_CHANNELS = ['Field Sales', 'Referral', 'Digital Marketing', 'App Store Download', 'Merchant Event', 'Partner Bank']

CITY_TIERS = {
    'Tier 1': ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', 'Ahmedabad'],
    'Tier 2': ['Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Visakhapatnam', 'Patna', 'Surat', 'Vadodara'],
    'Tier 3': ['Agra', 'Varanasi', 'Meerut', 'Nasik', 'Jabalpur', 'Amritsar', 'Dhanbad', 'Aurangabad', 'Ranchi', 'Coimbatore']
}

STATES_DISTRICTS = {
    'Maharashtra': ['Mumbai City', 'Pune', 'Nagpur', 'Thane', 'Nashik', 'Aurangabad'],
    'Delhi': ['Central Delhi', 'East Delhi', 'West Delhi', 'North Delhi', 'South Delhi', 'New Delhi'],
    'Karnataka': ['Bangalore Urban', 'Mysore', 'Hubli', 'Mangalore', 'Belgaum'],
    'Tamil Nadu': ['Chennai', 'Coimbatore', 'Madurai', 'Salem', 'Trichy'],
    'West Bengal': ['Kolkata', 'Howrah', 'Asansol', 'Durgapur', 'Siliguri'],
    'Uttar Pradesh': ['Lucknow', 'Kanpur', 'Agra', 'Varanasi', 'Meerut', 'Ghaziabad', 'Noida'],
    'Gujarat': ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot', 'Gandhinagar'],
    'Rajasthan': ['Jaipur', 'Jodhpur', 'Udaipur', 'Kota', 'Ajmer'],
    'Punjab': ['Amritsar', 'Ludhiana', 'Jalandhar', 'Patiala', 'Bathinda']
}

PIN_CODE_RANGES = {
    'Maharashtra': (400000, 445000),
    'Delhi': (110000, 110100),
    'Karnataka': (560000, 585000),
    'Tamil Nadu': (600000, 640000),
    'West Bengal': (700000, 740000),
    'Uttar Pradesh': (200000, 285000),
    'Gujarat': (360000, 396000),
    'Rajasthan': (300000, 345000),
    'Punjab': (140000, 165000)
}

DEVICE_TYPES = ['Android Low-End', 'Android Mid-Range', 'Android High-End', 'iPhone']

def generate_merchants(num_merchants=5000):
    merchant_list = []
    onboarding_dates = generate_onboarding_dates(num_merchants)
    for i in range(num_merchants):
        merchant_id = f"BPM{100000 + i}"
        business_name = generate_business_name()
        business_category, subcategory = generate_business_category()
        location_data = generate_merchant_location()
        onboarding_date = onboarding_dates[i]
        days_since_onboarding = (datetime.now() - onboarding_date).days
        acquisition_channel = random.choices(
            ACQUISITION_CHANNELS, 
            weights=[0.5, 0.15, 0.15, 0.1, 0.05, 0.05],
            k=1
        )[0]
        device_type = random.choices(
            DEVICE_TYPES,
            weights=[0.4, 0.4, 0.15, 0.05],
            k=1
        )[0]
        monthly_tx_count, monthly_tx_value, avg_ticket = generate_transaction_metrics(
            business_category, 
            subcategory, 
            location_data['tier'],
            onboarding_date
        )
        active_status, last_tx_date = determine_active_status(onboarding_date, business_category)
        qr_displayed, soundbox_adopted, swipe_machine = determine_product_adoption(
            active_status, 
            days_since_onboarding,
            monthly_tx_value
        )
        loans_taken, loan_status = determine_loan_status(
            active_status,
            monthly_tx_value,
            days_since_onboarding
        )
        merchant = {
            'merchant_id': merchant_id,
            'business_name': business_name,
            'business_category': business_category,
            'subcategory': subcategory,
            'city': location_data['city'],
            'district': location_data['district'],
            'state': location_data['state'],
            'pin_code': location_data['pin_code'],
            'tier': location_data['tier'],
            'onboarding_date': onboarding_date.strftime('%Y-%m-%d'),
            'acquisition_channel': acquisition_channel,
            'device_type': device_type,
            'active_status': active_status,
            'last_transaction_date': last_tx_date.strftime('%Y-%m-%d'),
            'qr_displayed': 'Yes' if qr_displayed else 'No',
            'soundbox_adopted': 'Yes' if soundbox_adopted else 'No',
            'swipe_machine': 'Yes' if swipe_machine else 'No',
            'loans_taken': loans_taken,
            'current_loan_status': loan_status,
            'monthly_transaction_count': monthly_tx_count,
            'monthly_transaction_value': monthly_tx_value,
            'avg_ticket_size': avg_ticket
        }
        merchant_list.append(merchant)
    return pd.



# =======Generate synthetic data about BharatPe merchant feature usage.======

For each merchant, simulates which features they use and how frequently,
based on their activity status.
"""

import random
import pandas as pd

def generate_feature_usage(merchants_df):
    features = ['QR Payments', 'Soundbox Alerts', 'Business Reports', 
                'Settlement History', 'Loan Dashboard', 'Rewards']
    
    usage_data = []
    
    for _, merchant in merchants_df.iterrows():
        merchant_id = merchant['merchant_id']
        
        if merchant['active_status'] == 'Active':
            num_features = random.randint(3, 6)
        elif merchant['active_status'] == 'Dormant':
            num_features = random.randint(1, 3)
        else:
            num_features = random.randint(0, 2)
            
        used_features = random.sample(features, num_features)
        
        for feature in features:
            is_used = feature in used_features
            
            if not is_used:
                frequency = 0
            elif feature == 'QR Payments':
                frequency = random.randint(15, 30)
            elif feature in ['Business Reports', 'Loan Dashboard']:
                frequency = random.randint(1, 8)
            else:
                frequency = random.randint(5, 20)
                
            usage_data.append({
                'merchant_id': merchant_id,
                'feature': feature,
                'is_used': 'Yes' if is_used else 'No',
                'monthly_frequency': frequency
            })
    
    return pd.DataFrame(usage_data)


# ======Synthetic BharatPe Dataset Generator =======

Generates realistic synthetic datasets including merchant profiles, transactions,
customer interactions, loans, feature usage, and geographic enrichments.


import os
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('en_IN')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

business_prefixes   = ['Sri','Shri','Shree','New','Modern','Super','Mega','Royal','Golden']
business_suffixes   = ['& Sons','& Co.','Enterprises','Industries','Mart','Shop','Traders','Dukaan','Emporium']
business_types      = ['Store','Shop','Traders','Enterprise','Mart','Center','Point','Junction','Dukan','Emporium']
used_business_names = set()

def generate_business_name():
    templates = [
      "{last_name} {suffix}",
      "{prefix} {business_type}",
      "{first_name} {business_type}",
      "{last_name} {business_type}",
      "{prefix} {suffix}"
    ]
    while True:
        name = random.choice(templates).format(
            first_name    = fake.first_name(),
            last_name     = fake.last_name(),
            prefix        = random.choice(business_prefixes),
            suffix        = random.choice(business_suffixes),
            business_type = random.choice(business_types)
        )
        if name not in used_business_names:
            used_business_names.add(name)
            return name

BUSINESS_CATEGORIES = {
    'Retail':       ['General Store','Grocery','Clothing','Electronics','Hardware','Mobile Shop','Medical Store','Stationery'],
    'Food & Beverage':['Restaurant','Cafe','Sweet Shop','Bakery','Tea Stall','Juice Center','Dhaba','Fast Food'],
    'Services':     ['Salon','Tailor','Laundry','Mobile Repair','Electronics Repair','Printing Services','Travel Agency'],
    'Wholesale':    ['Grocery Wholesale','Textile Wholesale','Electronics Wholesale','Stationery Wholesale'],
}
ACQUISITION_CHANNELS = ['Field Sales','Referral','Digital Marketing','App Store Download','Merchant Event','Partner Bank']
DEVICE_TYPES        = ['Android Low-End','Android Mid-Range','Android High-End','iPhone']

CITY_TIERS = {
  'Tier 1':['Mumbai','Delhi','Bangalore','Chennai','Kolkata','Hyderabad','Pune','Ahmedabad'],
  'Tier 2':['Jaipur','Lucknow','Kanpur','Nagpur','Indore','Bhopal','Visakhapatnam','Patna','Surat','Vadodara'],
  'Tier 3':['Agra','Varanasi','Meerut','Nasik','Jabalpur','Amritsar','Dhanbad','Aurangabad','Ranchi','Coimbatore']
}
STATES_DISTRICTS = {
  'Maharashtra':['Mumbai City','Pune','Nagpur','Thane','Nashik','Aurangabad'],
  'Delhi':['Central Delhi','East Delhi','West Delhi','North Delhi','South Delhi','New Delhi'],
  'Karnataka':['Bangalore Urban','Mysore','Hubli','Mangalore','Belgaum'],
  'Tamil Nadu':['Chennai','Coimbatore','Madurai','Salem','Trichy'],
  'West Bengal':['Kolkata','Howrah','Asansol','Durgapur','Siliguri'],
  'Uttar Pradesh':['Lucknow','Kanpur','Agra','Varanasi','Meerut','Ghaziabad','Noida'],
  'Gujarat':['Ahmedabad','Surat','Vadodara','Rajkot','Gandhinagar'],
  'Rajasthan':['Jaipur','Jodhpur','Udaipur','Kota','Ajmer'],
  'Punjab':['Amritsar','Ludhiana','Jalandhar','Patiala','Bathinda']
}
PIN_CODE_RANGES = {
  'Maharashtra':(400000,445000),'Delhi':(110000,110100),'Karnataka':(560000,585000),
  'Tamil Nadu':(600000,640000),'West Bengal':(700000,740000),
  'Uttar Pradesh':(200000,285000),'Gujarat':(360000,396000),
  'Rajasthan':(300000,345000),'Punjab':(140000,165000)
}
STATE_PENETRATION = {
  'Maharashtra':0.25,'Delhi':0.35,'Karnataka':0.22,'Tamil Nadu':0.18,
  'West Bengal':0.15,'Uttar Pradesh':0.12,'Gujarat':0.20,
  'Rajasthan':0.14,'Punjab':0.17
}
COMPETITIVE_INTENSITY = {'Tier 1':0.8,'Tier 2':0.5,'Tier 3':0.3}
FEATURES = ['QR Payments','Soundbox Alerts','Business Reports','Settlement History','Loan Dashboard','Rewards']

def generate_merchant_location():
    state    = random.choice(list(STATES_DISTRICTS))
    district = random.choice(STATES_DISTRICTS[state])
    tier     = next((t for t,c in CITY_TIERS.items() if district in c), None)
    if tier is None:
        tier = random.choices(['Tier 1','Tier 2','Tier 3'], [0.2,0.3,0.5])[0]
        district = random.choice(STATES_DISTRICTS[state])
    city = district
    if state in PIN_CODE_RANGES:
        lo,hi = PIN_CODE_RANGES[state]
        pin_code = random.randint(lo,hi)
    else:
        pin_code = random.randint(100000,999999)
    return {'state':state,'district':district,'city':city,'tier':tier,'pin_code':pin_code}

def generate_business_category():
    cat = random.choice(list(BUSINESS_CATEGORIES))
    sub = random.choice(BUSINESS_CATEGORIES[cat])
    return cat, sub

def generate_onboarding_dates(n):
    end   = datetime.now()
    start = end - timedelta(days=3*365)
    span  = (end-start).days
    days  = np.random.beta(2,5,n) * span
    return [start + timedelta(days=int(d)) for d in days]

def generate_transaction_metrics(business_category, subcategory, tier, onboarding_date):
    if business_category == 'Retail':
        base_count, base_value = random.randint(300,800), random.randint(50000,150000)
    elif business_category == 'Food & Beverage':
        base_count, base_value = random.randint(500,1200), random.randint(75000,200000)
    elif business_category == 'Services':
        base_count, base_value = random.randint(100,400), random.randint(30000,100000)
    else:
        base_count, base_value = random.randint(50,200), random.randint(200000,500000)

    if tier=='Tier 1':
        cm, vm = random.uniform(1.2,1.5), random.uniform(1.3,1.7)
    elif tier=='Tier 2':
        cm, vm = random.uniform(0.8,1.2), random.uniform(0.9,1.3)
    else:
        cm, vm = random.uniform(0.5,0.9), random.uniform(0.6,1.0)

    days_since = (datetime.now()-onboarding_date).days
    tenure_factor = min(1.0, days_since/365)

    count = int(base_count * cm * tenure_factor)
    value = int(base_value * vm * tenure_factor)
    count = int(count * random.uniform(0.85,1.15))
    value = int(value * random.uniform(0.85,1.15))
    avg_ticket = round(value / count if count>0 else 0, 2)
    return count, value, avg_ticket

def determine_active_status(onboarding_date, business_category):
    days = (datetime.now()-onboarding_date).days
    if days<30:   p=0.15
    elif days<90: p=0.08
    elif days<365:p=0.05
    else:         p=0.03
    if business_category=='Retail':   p*=0.9
    elif business_category=='Services':p*=1.1
    elif business_category=='Wholesale':p*=0.8

    r = random.random()
    if r<p:
        status='Churned'; last_days=random.randint(30,180)
    elif r<p+0.12:
        status='Dormant'; last_days=random.randint(30,90)
    else:
        status='Active'; last_days=random.randint(0,30)
    last_tx = datetime.now() - timedelta(days=last_days)
    return status, last_tx

def determine_product_adoption(status, days_since, monthly_value):
    qr_p, sb_p, sw_p = 0.95, 0.35, 0.25
    sb_p *= min(1.0, days_since/180)
    sw_p *= min(1.0, days_since/180)
    if monthly_value>200000: sb_p*=1.5; sw_p*=2.0
    elif monthly_value>100000: sb_p*=1.3; sw_p*=1.5
    elif monthly_value>50000: sb_p*=1.1; sw_p*=1.2
    else: sb_p*=0.9; sw_p*=0.8

    if status=='Churned': sb_p*=0.5; sw_p*=0.3
    elif status=='Dormant': sb_p*=0.7; sw_p*=0.5
    return (
        random.random() < qr_p,
        random.random()


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

