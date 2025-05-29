CREATE DATABASE paytm;

USE paytm;

CREATE TABLE reviews (
    content TEXT,
    score INT,
    thumbsUpCount INT,
    review_created_version VARCHAR(50),
    at DATETIME,
    appVersion VARCHAR(50)
);

-- 1. Basic Sentiment Classification
CREATE VIEW sentiment_analysis AS
SELECT
    content,
    score,
    at AS review_date,
    thumbsUpCount AS thumbs_up_count,
    review_created_version,
    appVersion AS app_version,
    CASE
        WHEN score <= 2 THEN 'Negative'
        WHEN score = 3 THEN 'Neutral'
        WHEN score > 3 THEN 'Positive'
    END AS sentiment_category
FROM reviews;


-- 2. Sentiment Distribution
CREATE VIEW sentiment_distribution AS
SELECT
    sentiment_category,
    COUNT(*) AS review_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reviews), 2) AS percentage
FROM sentiment_analysis
GROUP BY sentiment_category
ORDER BY percentage DESC;


-- 3. Top Mentioned Features (adjust feature list if needed)
CREATE VIEW feature_mentions AS
WITH feature_list AS (
    SELECT 'UPI' AS feature
    UNION SELECT 'payment device'
    UNION SELECT 'voice alert'
    UNION SELECT 'business loan'
    UNION SELECT 'loan approval'
    UNION SELECT 'credit card'
    UNION SELECT 'QR code'
    UNION SELECT 'merchant'
    UNION SELECT 'settlement'
    UNION SELECT 'customer care'
    UNION SELECT 'app'
    UNION SELECT 'scanner'
    UNION SELECT 'payment'
    UNION SELECT 'transaction'
    UNION SELECT 'cashback'
    UNION SELECT 'support'
    UNION SELECT 'service'
    UNION SELECT 'paytm'
    UNION SELECT 'wallet'
    UNION SELECT 'refund'
    UNION SELECT 'disbursement'
    UNION SELECT 'loan rejection'
    UNION SELECT 'penalty'
    UNION SELECT 'feature request'
)
SELECT
    f.feature,
    COUNT(*) AS mention_count,
    ROUND(AVG(r.score), 2) AS avg_rating,
    ROUND(AVG(CASE WHEN sa.sentiment_category = 'Positive' THEN 1.0 ELSE 0.0 END), 2) AS positive_sentiment_ratio,
    ROUND(AVG(CASE WHEN sa.sentiment_category = 'Negative' THEN 1.0 ELSE 0.0 END), 2) AS negative_sentiment_ratio,
    ROUND(AVG(CASE WHEN sa.sentiment_category = 'Neutral' THEN 1.0 ELSE 0.0 END), 2) AS neutral_sentiment_ratio
FROM feature_list f
JOIN reviews r ON LOWER(r.content) LIKE CONCAT('%', LOWER(f.feature), '%')
JOIN sentiment_analysis sa ON r.content = sa.content AND r.at = sa.review_date
GROUP BY f.feature
ORDER BY mention_count DESC;


-- 4. Pain Points Extraction
CREATE OR REPLACE VIEW pain_points_summary AS
WITH negative_reviews AS (
    SELECT content, score
    FROM sentiment_analysis
    WHERE sentiment_category = 'Negative'
),
issue_counts AS (
    SELECT
        CASE
            WHEN LOWER(content) LIKE '%payment%' OR LOWER(content) LIKE '%settlement%' OR LOWER(content) LIKE '%hold%' OR LOWER(content) LIKE '%credited%' OR LOWER(content) LIKE '%transfer%' OR LOWER(content) LIKE '%refund%' OR LOWER(content) LIKE '%pending%' OR LOWER(content) LIKE '%delay%' OR LOWER(content) LIKE '%processing%' THEN 'Payment & Settlement Issues'
            WHEN LOWER(content) LIKE '%loan%' OR LOWER(content) LIKE '%interest%' OR LOWER(content) LIKE '%emi%' OR LOWER(content) LIKE '%cibil%' OR LOWER(content) LIKE '%rejected%' OR LOWER(content) LIKE '%disburse%' OR LOWER(content) LIKE '%overdue%' OR LOWER(content) LIKE '%repayment%' OR LOWER(content) LIKE '%penalty%' OR LOWER(content) LIKE '%application%' OR LOWER(content) LIKE '%loan not given%' OR LOWER(content) LIKE '%no loan%' OR LOWER(content) LIKE '%loan rejection%' THEN 'Loan Issues'
            WHEN LOWER(content) LIKE '%customer care%' OR LOWER(content) LIKE '%support%' OR LOWER(content) LIKE '%help%' OR LOWER(content) LIKE '%response%' OR LOWER(content) LIKE '%call%' OR LOWER(content) LIKE '%chat%' OR LOWER(content) LIKE '%service center%' OR LOWER(content) LIKE '%representative%' OR LOWER(content) LIKE '%agent%' OR LOWER(content) LIKE '%no support%' OR LOWER(content) LIKE '%no help%' OR LOWER(content) LIKE '%not responding%' OR LOWER(content) LIKE '%call disconnect%' OR LOWER(content) LIKE '%response delay%' THEN 'Customer Support Issues'
            WHEN LOWER(content) LIKE '%app not working%' OR LOWER(content) LIKE '%crash%' OR LOWER(content) LIKE '%slow%' OR LOWER(content) LIKE '%login%' OR LOWER(content) LIKE '%error%' OR LOWER(content) LIKE '%bug%' OR LOWER(content) LIKE '%freeze%' OR LOWER(content) LIKE '%hang%' OR LOWER(content) LIKE '%not opening%' OR LOWER(content) LIKE '%unresponsive%' OR LOWER(content) LIKE '%not working%' OR LOWER(content) LIKE '%audio%' OR LOWER(content) LIKE '%app not open%' OR LOWER(content) LIKE '%app didnt open%' THEN 'App Performance Issues'
            WHEN LOWER(content) LIKE '%technical%' OR LOWER(content) LIKE '%glitch%' OR LOWER(content) LIKE '%network%' OR LOWER(content) LIKE '%server%' OR LOWER(content) LIKE '%connectivity%' OR LOWER(content) LIKE '%system%' OR LOWER(content) LIKE '%issue%' OR LOWER(content) LIKE '%failure%' THEN 'Technical Glitches'
            WHEN LOWER(content) LIKE '%charge%' OR LOWER(content) LIKE '%fees%' OR LOWER(content) LIKE '%rental%' OR LOWER(content) LIKE '%hidden%' OR LOWER(content) LIKE '%deduct%' OR LOWER(content) LIKE '%penalty%' OR LOWER(content) LIKE '%extra%' OR LOWER(content) LIKE '%cost%' OR LOWER(content) LIKE '%amount%' THEN 'Hidden Charges / Fees'
            WHEN LOWER(content) LIKE '%speaker%' OR LOWER(content) LIKE '%machine%' OR LOWER(content) LIKE '%device%' OR LOWER(content) LIKE '%swipe%' OR LOWER(content) LIKE '%pos%' OR LOWER(content) LIKE '%equipment%' OR LOWER(content) LIKE '%scanner%' THEN 'Equipment Issues'
            WHEN LOWER(content) LIKE '%fraud%' OR LOWER(content) LIKE '%cheat%' OR LOWER(content) LIKE '%fake%' OR LOWER(content) LIKE '%scam%' OR LOWER(content) LIKE '%misuse%' OR LOWER(content) LIKE '%crook%' OR LOWER(content) LIKE '%thief%' OR LOWER(content) LIKE '%ripoff%' OR LOWER(content) LIKE '%froud%' THEN 'Fraud / Trust Concerns'
            WHEN LOWER(content) LIKE '%notification%' OR LOWER(content) LIKE '%voice alert%' OR LOWER(content) LIKE '%alert%' OR LOWER(content) LIKE '%sms%' OR LOWER(content) LIKE '%message%' OR LOWER(content) LIKE '%alert sound%' THEN 'Notification Problems'
            WHEN LOWER(content) LIKE '%kyc%' OR LOWER(content) LIKE '%verification%' OR LOWER(content) LIKE '%document%' OR LOWER(content) LIKE '%reject%' OR LOWER(content) LIKE '%approval%' OR LOWER(content) LIKE '%dispute%' OR LOWER(content) LIKE '%identity%' OR LOWER(content) LIKE '%upload%' OR LOWER(content) LIKE '%document upload%' OR LOWER(content) LIKE '%document rejection%' OR LOWER(content) LIKE '%verification delay%' THEN 'KYC / Verification Issues'
            WHEN LOWER(content) LIKE '%blocked%' OR LOWER(content) LIKE '%block%' OR LOWER(content) LIKE '%disable%' OR LOWER(content) LIKE '%account locked%' OR LOWER(content) LIKE '%suspended%' OR LOWER(content) LIKE '%account freeze%' THEN 'Account Blocking Issues'
            WHEN LOWER(content) LIKE '%sales%' OR LOWER(content) LIKE '%offer%' OR LOWER(content) LIKE '%misleading%' OR LOWER(content) LIKE '%promotion%' OR LOWER(content) LIKE '%target%' OR LOWER(content) LIKE '%agent%' OR LOWER(content) LIKE '%marketing%' THEN 'Misleading Sales & Offers'
            WHEN LOWER(content) LIKE '%policy%' OR LOWER(content) LIKE '%process%' OR LOWER(content) LIKE '%rule%' OR LOWER(content) LIKE '%terms%' OR LOWER(content) LIKE '%condition%' OR LOWER(content) LIKE '%procedure%' OR LOWER(content) LIKE '%guideline%' OR LOWER(content) LIKE '%requirement%' THEN 'Process & Policy Complaints'
            WHEN LOWER(content) LIKE '%communication%' OR LOWER(content) LIKE '%call disconnect%' OR LOWER(content) LIKE '%response delay%' OR LOWER(content) LIKE '%no reply%' OR LOWER(content) LIKE '%not answering%' OR LOWER(content) LIKE '%contact%' OR LOWER(content) LIKE '%no response%' OR LOWER(content) LIKE '%no answer%' THEN 'Communication Issues'
            WHEN LOWER(content) LIKE '%service%' OR LOWER(content) LIKE '%poor%' OR LOWER(content) LIKE '%bad%' OR LOWER(content) LIKE '%worst%' OR LOWER(content) LIKE '%not good%' OR LOWER(content) LIKE '%unsatisfactory%' OR LOWER(content) LIKE '%unsupportive%' THEN 'General Service Complaints'
            WHEN LOWER(content) LIKE '%privacy%' OR LOWER(content) LIKE '%data leak%' OR LOWER(content) LIKE '%security%' OR LOWER(content) LIKE '%permission%' OR LOWER(content) LIKE '%access%' THEN 'Privacy & Security Concerns'
            WHEN LOWER(content) LIKE '%feature request%' OR LOWER(content) LIKE '%missing feature%' OR LOWER(content) LIKE '%improvement%' OR LOWER(content) LIKE '%suggestion%' OR LOWER(content) LIKE '%ui%' OR LOWER(content) LIKE '%user interface%' THEN 'Feature Requests & UI Issues'
            WHEN LOWER(content) LIKE '%loan approval%' OR LOWER(content) LIKE '%eligibility%' OR LOWER(content) LIKE '%waiting time%' OR LOWER(content) LIKE '%disbursal%' OR LOWER(content) LIKE '%application%' THEN 'Loan Approval & Eligibility Issues'
            WHEN LOWER(content) LIKE '%account update%' OR LOWER(content) LIKE '%profile%' OR LOWER(content) LIKE '%change details%' OR LOWER(content) LIKE '%update%' OR LOWER(content) LIKE '%modify%' THEN 'Account Management Issues'
            WHEN LOWER(content) LIKE '%refund delay%' OR LOWER(content) LIKE '%refund not received%' OR LOWER(content) LIKE '%money stuck%' OR LOWER(content) LIKE '%not credited%' OR LOWER(content) LIKE '%payment not received%' THEN 'Refund Issues'
            WHEN LOWER(content) LIKE '%cancel%' OR LOWER(content) LIKE '%cancellation%' OR LOWER(content) LIKE '%withdraw%' OR LOWER(content) LIKE '%stop%' OR LOWER(content) LIKE '%terminate%' THEN 'Cancellation / Withdrawal Issues'
            WHEN LOWER(content) LIKE '%delay%' OR LOWER(content) LIKE '%waiting%' OR LOWER(content) LIKE '%slow%' OR LOWER(content) LIKE '%waiting time%' THEN 'Delay & Waiting Time Issues'
            WHEN LOWER(content) LIKE '%complaint%' OR LOWER(content) LIKE '%issue%' OR LOWER(content) LIKE '%problem%' OR LOWER(content) LIKE '%bad experience%' OR LOWER(content) LIKE '%unsatisfactory%' OR LOWER(content) LIKE '%disappointed%' OR LOWER(content) LIKE '%frustrated%' THEN 'General Complaints'
            ELSE 'Other Issues'
        END AS issue_category,
        score
    FROM negative_reviews
)
SELECT issue_category, COUNT(*) AS issue_count, ROUND(AVG(score), 2) AS avg_score, ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM issue_counts), 2) AS percentage
FROM issue_counts
GROUP BY issue_category
ORDER BY issue_count DESC;


-- 5. Daily Sentiment Trend
CREATE VIEW daily_sentiment_trend AS
SELECT
    DATE(review_date) AS review_day,
    COUNT(*) AS total_reviews,
    ROUND(AVG(score), 2) AS avg_rating,
    SUM(CASE WHEN sentiment_category = 'Positive' THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN sentiment_category = 'Neutral' THEN 1 ELSE 0 END) AS neutral_reviews,
    SUM(CASE WHEN sentiment_category = 'Negative' THEN 1 ELSE 0 END) AS negative_reviews,
    ROUND(SUM(CASE WHEN sentiment_category = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS positive_percentage
FROM sentiment_analysis
GROUP BY review_day
ORDER BY review_day DESC;


-- 6. Version Performance Analysis
CREATE VIEW version_performance AS
SELECT
    review_created_version,
    app_version,
    COUNT(*) AS total_reviews,
    ROUND(AVG(score), 2) AS avg_rating,
    SUM(CASE WHEN sentiment_category = 'Positive' THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN sentiment_category = 'Negative' THEN 1 ELSE 0 END) AS negative_reviews,
    ROUND(SUM(CASE WHEN sentiment_category = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS positive_percentage
FROM sentiment_analysis
GROUP BY review_created_version, app_version
ORDER BY avg_rating DESC;


-- 7. Most Helpful Reviews
CREATE VIEW most_helpful_reviews AS
SELECT
    content,
    score,
    thumbs_up_count,
    sentiment_category,
    review_date
FROM sentiment_analysis
ORDER BY thumbs_up_count DESC, score DESC
LIMIT 10;


-- 8. Language Analysis
CREATE VIEW language_analysis AS
SELECT
    CASE
        WHEN content LIKE '%vyapari%' OR content LIKE '%sahi nahin%' OR content LIKE '%लोन%' OR content LIKE '%सर्विस%' OR
             content LIKE '%jhakas%' OR content LIKE '%kamai%' OR content LIKE '%lootane%' THEN 'Hindi/Local'
        ELSE 'English'
    END AS language,
    COUNT(*) AS review_count,
    ROUND(AVG(score), 2) AS avg_rating,
    ROUND(SUM(CASE WHEN sentiment_category = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS positive_percentage
FROM sentiment_analysis
GROUP BY language
ORDER BY review_count DESC;


-- 9. Word Frequency Analysis
CREATE VIEW word_frequency AS
WITH word_split AS (
    SELECT
        TRIM(LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(r.content, ' ', n.n), ' ', -1))) AS word,
        r.score,
        sa.sentiment_category
    FROM reviews r
    JOIN (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 
          UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) n
    ON CHAR_LENGTH(r.content) - CHAR_LENGTH(REPLACE(r.content, ' ', '')) >= n.n - 1
    JOIN sentiment_analysis sa ON r.content = sa.content AND r.at = sa.review_date
    WHERE LENGTH(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(r.content, ' ', n.n), ' ', -1))) > 3
    AND TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(r.content, ' ', n.n), ' ', -1)) NOT IN ('this', 'that', 'with', 'from', 'have', 'what', 'your', 'they', 'will', 'would', 'could', 'when', 'where')
)
SELECT
    word,
    COUNT(*) AS frequency,
    ROUND(AVG(word_split.score), 2) AS avg_rating, 
    ROUND(SUM(CASE WHEN word_split.sentiment_category = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS positive_percentage
FROM word_split
GROUP BY word
HAVING COUNT(*) > 1
ORDER BY frequency DESC
LIMIT 30;