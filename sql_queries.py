# ---------- SONGPLAY SESSIONS -------------
drop_songplay_sessions = """DROP TABLE IF EXISTS songplay_sessions"""
create_songplay_sessions = """
        CREATE TABLE songplay_sessions
        (
        sessionId     int,
        itemInSession int,
        artist        text,
        song    text,
        song_length   double,
        PRIMARY KEY ((sessionId, itemInSession))
        )
"""

insert_songplay_sessions = """
INSERT INTO event_data.songplay_sessions (sessionid, iteminsession, artist, song, song_length)
VALUES (%s, %s, %s, %s, %s)
"""

""" 
Give me the 
# artist, song title and song’s length 
# that was heard during sessionId = 338, and itemInSession = 4
"""
select_songplay_sessions = """
SELECT artist, song, song_length
FROM event_data.songplay_sessions
where sessionid = %s 
and iteminsession=%s
"""

# ---------- USER SESSIONS -------------

drop_user_sessions = """DROP TABLE IF EXISTS user_sessions"""
create_user_sessions = """
    CREATE TABLE user_sessions
        (
        userId        int,
        sessionId     int,
        itemInSession int,
        firstName     text,
        lastName      text,
        artist text,
        song   text,
        PRIMARY KEY ((userId, sessionId), itemInSession)
        )
"""

insert_user_sessions = """
INSERT INTO event_data.user_sessions (userid, sessionid, iteminsession, firstname, lastname, artist, song)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

"""Give me only the following: 
name of artist, song (sorted by itemInSession) and user (first and last name) 
for userid = 10, sessionid = 182
"""
select_user_sessions = """
SELECT artist, song, itemInSession, firstname, lastname
FROM event_data.user_sessions
where userId = %s
and sessionId = %s
"""


# ---------- SONG SUBSCRIBERS -------------

drop_song_subscribers = """DROP TABLE IF EXISTS song_subscribers"""
create_song_subscribers = """
    CREATE TABLE song_subscribers
    (
    song      text,
    firstName text,
    lastName  text,
    PRIMARY KEY ((song), firstName, LastName)
    )
"""

insert_song_subscribers = """
INSERT INTO event_data.song_subscribers (song, firstname, lastname)
VALUES (%s, %s, %s)
"""

"""
Give me every user name (first and last) in my music app history who listened to the song ‘All Hands Against His Own
"""
select_song_subscribers = """
SELECT firstname, lastName
FROM event_data.song_subscribers 
where song = %s
"""


