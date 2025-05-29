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
        count=1000,
        continuation_token=token
    )
    all_reviews.extend(result)
    print(f'Fetched {len(all_reviews)} reviews so far...')

df = pd.DataFrame(all_reviews)
df.to_csv('bharatpe_reviews.csv', index=False)
df.to_excel('bharatpe_reviews.xlsx', index=False)
