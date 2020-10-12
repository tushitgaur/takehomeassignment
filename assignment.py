import sqlite3 as sl
import pandas as pd
import requests


def create_tables():
    con = sl.connect('tvmaze_db.db')
    with con:
        data = con.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='actor_table'")
        for row in data:
            data = row[0]
        if data == 0:
            con.execute(""" CREATE TABLE actor_table (
                         actor_id INTEGER, 
                         show_id INTEGER
                        )        
          """)

        data = con.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='show_table'")
        for row in data:
            data = row[0]
        if data == 0:
            con.execute(""" CREATE TABLE show_table (
                  show_id INTEGER PRIMARY KEY, 
                  name TEXT,
                  language TEXT,
                  summary TEXT,
                  genres TEXT,
                  num_seasons INTEGER,
                  episode_count_per_season FLOAT,
                  view_time_in_hr FLOAT               
                  )        
                  """)


def call_api_for_actor(actor_id):
    con = sl.connect('tvmaze_db.db')
    total_shows = []
    show_rating_dict = {}
    list_table_insert = []
    actor_shows = requests.get(f"http://api.tvmaze.com/people/{actor_id}/castcredits?embed=show")
    ## Identifying the Top 3 rated shows.
    for each_row in actor_shows.json():
        show_id = each_row['_embedded']['show']['id']
        show_rating = each_row['_embedded']['show']['rating']['average']
        if show_rating is None:
            show_rating = 0
        total_shows.append(show_rating)
        show_rating_dict[show_rating] = show_id
    total_shows.sort(reverse=True)
    for i in range(min(3, len(total_shows))):  ## what if no shows
        list_table_insert.append((actor_id, show_rating_dict[total_shows[i]]))

    sql = 'INSERT INTO actor_table  values(?,?)'
    con.executemany(sql, list_table_insert)
    con.commit()
    con.close()


def call_api_for_show(show_id):

    con = sl.connect('tvmaze_db.db')
    data_request = requests.get(f"http://api.tvmaze.com/shows/{show_id}").json()
    show_name = data_request['name']
    show_language = data_request['language']
    show_genres = ",".join(data_request['genres'])
    show_summary = data_request['summary']
    season_details = requests.get(f"http://api.tvmaze.com/shows/{show_id}/seasons").json()
    num_seasons = len(season_details)
    ep_count = 0
    viewing_time_required = 0
    for i in range(num_seasons):
        if season_details[i]['episodeOrder'] is None:
            ep_count += 0
        else:
            ep_count += season_details[i]['episodeOrder']
        ep_details = requests.get(f"http://api.tvmaze.com/seasons/{season_details[i]['id']}/episodes").json()
        for j in range(len(ep_details)):
            viewing_time_required += ep_details[j]["runtime"] / 60
    avg = ep_count / num_seasons
    # print((show_id,show_name,show_language,show_summary,show_genres,num_seasons,avg,viewing_time_required))

    sql = 'INSERT INTO show_table values(?,?,?,?,?,?,?,?)'
    con.executemany(sql, [
        (show_id, show_name, show_language, str(show_summary).replace(";", " "), show_genres, num_seasons, avg, viewing_time_required)])
    con.commit()
    con.close()


def actor_info(actor_id):
    con = sl.connect('tvmaze_db.db')
    data = con.execute(f""" SELECT distinct actor_id,show_id  FROM  actor_table  WHERE actor_id = {actor_id}""")
    data = data.fetchall()
    if len(data) == 0:
        call_api_for_actor(actor_id)
        data = con.execute(f""" SELECT distinct actor_id,show_id  FROM  actor_table  WHERE actor_id = {actor_id}""")
        data = data.fetchall()
        con.close()
        for row in data:
            print(f"Actor Info:{row}")
            show_info(row[1])



def show_info(show_id):
    con = sl.connect('tvmaze_db.db')
    data = con.execute(f""" SELECT  *  FROM  show_table  WHERE show_id = {show_id}""")
    data = data.fetchall()
    con.close()
    if len(data) == 0:
        call_api_for_show(show_id)


## Creating Data tables
create_tables()

actor_name_input_file = pd.read_csv("input.csv")
actor_name_input_file = actor_name_input_file["Actor names"].tolist()

print(actor_name_input_file)

actor_id = []
for names in actor_name_input_file:
    actor_id_response = requests.get(f"http://api.tvmaze.com/search/people/?q={names}").json()
    id = actor_id_response[0]['person']['id']
    print(names)
    print(id)
    actor_id.append(id)
    actor_info(id)




con = sl.connect('tvmaze_db.db')
actor_tuple = tuple(actor_id)
query = '''SELECT a.actor_id,b.*
FROM  actor_table  as a
left join show_table  as b
on a.show_id = b.show_id
WHERE a.actor_id in {}'''.format(actor_tuple).replace(",)", ")")

data = con.execute(query)
data = data.fetchall()

output_list = ['actor_id', 'show_id', 'Show Name', 'Language', 'Summary', 'Genres', 'Seasons', 'episodes_per_seasons',
               'view_time_hours']

df = pd.DataFrame(data, columns=output_list)
df.to_csv("output.csv", sep=";", index=False)


