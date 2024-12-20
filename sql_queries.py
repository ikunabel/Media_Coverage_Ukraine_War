"""
This file contains the SQL-queries used for the analyis.

The stance information of every tweet is formatted like this:
[{'hypothesis': 'This statement is in favour of Russia', 'contra_prob': '0.9606', 'entail_prob': '0.0394'}, 
 {'hypothesis': 'This statement is against Russia', 'contra_prob': '0.9584', 'entail_prob': '0.0416'}, 
 {'hypothesis': 'This statement is against Ukraine', 'contra_prob': '0.9729', 'entail_prob': '0.0271'}, 
 {'hypothesis': 'This statement is in favour of Ukraine', 'contra_prob': '0.9866', 'entail_prob': '0.0134'}, 
 {'hypothesis': 'This statement is in favour of war', 'contra_prob': '0.9883', 'entail_prob': '0.0117'}, 
 {'hypothesis': 'This statement is against war', 'contra_prob': '0.9976', 'entail_prob': '0.0024'}, 
 {'hypothesis': 'This statement is in favour of military conflict', 'contra_prob': '0.9646', 'entail_prob': '0.0354'}, 
 {'hypothesis': 'This statement is against military conflict', 'contra_prob': '0.9973', 'entail_prob': '0.0027'}]

 How to use? Use a query as argument for run_custom_query(conn, query) function.
"""

# How many distinct countries?
query = """
SELECT 
    COUNT(DISTINCT country) AS distinct_countries
FROM records
WHERE country IS NOT NULL
"""

# How many tweets from every country?
query = """
SELECT 
    country, 
    COUNT(*) AS entry_count
FROM records
WHERE country IS NOT NULL
GROUP BY country
ORDER BY entry_count DESC
"""

# Which countries' outlets support the given hypothesis?
query = """
SELECT 
    country, 
    avg(json_extract(value, '$.entail_prob')) AS avg_entail_prob
FROM records, 
    json_each(records.stance)
WHERE 
    stance IS NOT NULL 
    AND json_extract(value, '$.hypothesis') = 'This statement is in favour of Russia'
GROUP BY country
ORDER BY avg_entail_prob DESC
"""

# Outlets with corresponding country and stance for each hypothesis
query = """
SELECT 
    records.channel AS media_outlet, 
    records.country, 
    COUNT(*) AS tweet_count,

    -- Average entail probabilities for all eight hypotheses
    AVG(CASE WHEN json_extract(value, '$.hypothesis') = 'This statement is in favour of Russia' THEN json_extract(value, '$.entail_prob') ELSE NULL END) AS avg_favor_russia,
    AVG(CASE WHEN json_extract(value, '$.hypothesis') = 'This statement is against Russia' THEN json_extract(value, '$.entail_prob') ELSE NULL END) AS avg_against_russia,
    AVG(CASE WHEN json_extract(value, '$.hypothesis') = 'This statement is in favour of Ukraine' THEN json_extract(value, '$.entail_prob') ELSE NULL END) AS avg_favor_ukraine,
    AVG(CASE WHEN json_extract(value, '$.hypothesis') = 'This statement is against Ukraine' THEN json_extract(value, '$.entail_prob') ELSE NULL END) AS avg_against_ukraine,
    AVG(CASE WHEN json_extract(value, '$.hypothesis') = 'This statement is in favour of war' THEN json_extract(value, '$.entail_prob') ELSE NULL END) AS avg_favor_war,
    AVG(CASE WHEN json_extract(value, '$.hypothesis') = 'This statement is against war' THEN json_extract(value, '$.entail_prob') ELSE NULL END) AS avg_against_war,
    AVG(CASE WHEN json_extract(value, '$.hypothesis') = 'This statement is in favour of military conflict' THEN json_extract(value, '$.entail_prob') ELSE NULL END) AS avg_favor_military_conflict,
    AVG(CASE WHEN json_extract(value, '$.hypothesis') = 'This statement is against military conflict' THEN json_extract(value, '$.entail_prob') ELSE NULL END) AS avg_against_military_conflict

FROM records, json_each(records.stance)
WHERE 
    records.channel IS NOT NULL AND records.channel != '' 
    AND records.country IS NOT NULL AND records.country != ''
GROUP BY records.channel, records.country
ORDER BY tweet_count DESC
"""

# Average stances of European countries
query = """
WITH stance_data AS (
    SELECT 
        country AS Country, 
        COUNT(*) AS Tweet_Count,
        SUM(pro_russia) AS Pro_Russia,
        SUM(pro_ukraine) AS Pro_Ukraine,
        SUM(unsure) AS Unsure
    FROM records
    WHERE country IN (
        'France', 'Germany', 'Italy', 'Spain', 'United Kingdom', 'Poland', 
        'Ukraine', 'Netherlands', 'Sweden', 'Denmark', 'Norway', 'Austria', 
        'Belgium', 'Hungary', 'Bulgaria', 'Czech Republic', 'Finland', 'Greece', 
        'Iceland', 'Luxembourg', 'Portugal', 'Romania', 'Russia', 
        'Republic of Ireland', 'Slovakia', 'Switzerland', 'Turkey'
    )
    GROUP BY country
    ORDER BY Tweet_Count DESC
)

SELECT * FROM stance_data;

"""

# Average stances of all countries
query= """
WITH stance_data AS (
    SELECT 
        country AS Country, 
        COUNT(*) AS Tweet_Count,
        SUM(pro_russia) AS Pro_Russia,
        SUM(pro_ukraine) AS Pro_Ukraine,
        SUM(unsure) AS Unsure
    FROM records
    GROUP BY country
    ORDER BY Tweet_Count DESC
)

SELECT * FROM stance_data;
"""

# Average stances of all media outlets
query= """
WITH stance_data AS (
    SELECT 
        channel AS Media_Outlet, 
        COUNT(*) AS Tweet_Count,
        SUM(pro_russia) AS Pro_Russia,
        SUM(pro_ukraine) AS Pro_Ukraine,
        SUM(unsure) AS Unsure
    FROM records
    GROUP BY channel
    ORDER BY Tweet_Count DESC
)

SELECT * FROM stance_data;
"""



