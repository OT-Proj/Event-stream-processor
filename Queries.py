# this file contains query constants.
# they are isolated from out main classes for readability purposes.

# please note that all queries DO support multiple (non-concurrent) sessions per user


# query all users to find the 10 most converting users
# the query would not result users that had no conversions
TOP_10_CONVERSERS_QUERY = """
SELECT es1.user_id, count(es1.type) AS conversions
FROM event_stream AS es1
WHERE es1.type = "conversion"
GROUP BY es1.user_id
ORDER BY conversions desc
LIMIT 10
"""

# measures the distances between session_start and conversion events per session.
# user distance is defined as the minimum distance measured at ANY session.
# we distinctly return only one result per user
FAST_CONVERTING_USERS = """
SELECT DISTINCT user_id, min(distance) AS distance
FROM (
        SELECT es1.user_id, count(es1.type) AS distance
        FROM event_stream AS es1, event_stream AS es2
        WHERE (es1.type = "in_page" AND
               es2.type = "conversion" AND
               es1.session_id = es2.session_id)
        GROUP BY es1.user_id, es1.session_id
    )
GROUP BY user_id
ORDER BY distance desc
"""

# calculates the average number of events until a conversion within all sessions
# it also returns the number of sessions recorded, and number of conversions recorded,
# in order to clarify whether or not the average number really represents our application's success.
AVG_CONVERSION_DISTANCE = """
SELECT avg(conversion_distances.distance) AS avg_distance
FROM (
        SELECT es1.user_id, count(es1.type) AS distance
        FROM event_stream AS es1, event_stream AS es2
        WHERE (es1.type = "in_page" AND
               es2.type = "conversion" AND
               es1.session_id = es2.session_id)
        GROUP BY es1.user_id, es1.session_id
    ) AS conversion_distances
"""

UNSUCCESSFUL_CONVERSIONS_RATIO = """
SELECT *
FROM (
        SELECT count(es1.type) AS number_of_conversions
        FROM event_stream AS es1
        WHERE type = "conversion"
     )
CROSS JOIN (
                SELECT count(session_id) as number_of_unsuccessful_conversions
                FROM (
                        SELECT es1.session_id
                        FROM event_stream AS es1
                        WHERE NOT EXISTS (
                                            SELECT *
                                            FROM event_stream AS es2
                                            WHERE es1.session_id = es2.session_id AND
                                                  es2.type = "conversion"
                                         )
                        GROUP BY es1.session_id
                     )
           ) 
"""

PATTERN_RECOGNITION_PREPARATION = """
SELECT session_id, user_id, url
FROM event_stream AS es1
WHERE es1.type = "in_page"
ORDER BY es1.timestamp asc
"""