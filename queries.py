# Created by Gabriel Martell

'''
Version 1.2 (04/13/2024)
=========================================================
queries.py (Carleton University COMP3005 - Database Management Student Template Code)

This is the template code for the COMP3005 Database Project 1, and must be accomplished on an Ubuntu Linux environment.
Your task is to ONLY write your SQL queries within the prompted space within each Q_# method (where # is the question number).

You may modify code in terms of testing purposes (commenting out a Qn method), however, any alterations to the code, such as modifying the time, 
will be flagged for suspicion of cheating - and thus will be reviewed by the staff and, if need be, the Dean. 

To review the Integrity Violation Attributes of Carleton University, please view https://carleton.ca/registrar/academic-integrity/ 

=========================================================
'''

# Imports
import psycopg
import csv
import subprocess
import os
import re

# Connection Information
''' 
The following is the connection information for this project. These settings are used to connect this file to the autograder.
You must NOT change these settings - by default, db_host, db_port and db_username are as follows when first installing and utilizing psql.
For the user "postgres", you must MANUALLY set the password to 1234.

This can be done with the following snippet:

sudo -u postgres psql
\password postgres

'''
root_database_name = "project_database"
query_database_name = "query_database"
db_username = 'postgres'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'

# Directory Path - Do NOT Modify
dir_path = os.path.dirname(os.path.realpath(__file__))

# Loading the Database after Drop - Do NOT Modify
# ================================================


def load_database(conn):
    drop_database(conn)

    cursor = conn.cursor()
    # Create the Database if it DNE
    try:
        conn.autocommit = True
        cursor.execute(f"CREATE DATABASE {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)

    finally:
        cursor.close()
        conn.autocommit = False
    conn.close()

    # Connect to this query database.
    dbname = query_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    conn = psycopg.connect(dbname=dbname, user=user,
                           password=password, host=host, port=port)

    # Import the dbexport.sql database data into this database
    try:
        command = f'psql -h {host} -U {user} -d {query_database_name} -a -f "{
            os.path.join(dir_path, "dbexport.sql")}" > /dev/null 2>&1'
        env = {'PGPASSWORD': password}
        subprocess.run(command, shell=True, check=True, env=env)

    except Exception as error:
        print(f"An error occurred while loading the database: {error}")

    # Return this connection.
    return conn

# Dropping the Database after Query n Execution - Do NOT Modify
# ================================================


def drop_database(conn):
    # Drop database if it exists.

    cursor = conn.cursor()

    try:
        conn.autocommit = True
        cursor.execute(f"DROP DATABASE IF EXISTS {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)
        pass

    finally:
        cursor.close()
        conn.autocommit = False

# Reconnect to Root Database - Do NOT Modify
# ================================================


def reconnect():
    dbname = root_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    return psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# Getting the execution time of the query through EXPLAIN ANALYZE - Do NOT Modify
# ================================================


def get_time(cursor, sql_query):
    # Prefix your query with EXPLAIN ANALYZE
    explain_query = f"EXPLAIN ANALYZE {sql_query}"

    try:
        # Execute the EXPLAIN ANALYZE query
        cursor.execute(explain_query)

        # Fetch all rows from the cursor
        explain_output = cursor.fetchall()

        # Convert the output tuples to a single string
        explain_text = "\n".join([row[0] for row in explain_output])

        # Use regular expression to find the execution time
        # Look for the pattern "Execution Time: <time> ms"
        match = re.search(r"Execution Time: ([\d.]+) ms", explain_text)
        if match:
            execution_time = float(match.group(1))
            return f"Execution Time: {execution_time} ms"
        else:
            print("Execution Time not found in EXPLAIN ANALYZE output.")
            return f"NA"

    except Exception as error:
        print(f"[ERROR] Error getting time.\n{error}")


# Write the results into some Q_n CSV. If the is an error with the query, it is a INC result - Do NOT Modify
# ================================================
def write_csv(execution_time, cursor, i):
    # Collect all data into this csv, if there is an error from the query execution, the resulting time is INC.
    try:
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        filename = f"{dir_path}/Q_{i}.csv"

        with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)

            # Write column names to the CSV file
            csvwriter.writerow(colnames)

            # Write data rows to the CSV file
            csvwriter.writerows(rows)

    except Exception as error:
        execution_time[i-1] = "INC"
        print(error)

# ================================================


'''
The following 10 methods, (Q_n(), where 1 < n < 10) will be where you are tasked to input your queries.
To reiterate, any modification outside of the query line will be flagged, and then marked as potential cheating.
Once you run this script, these 10 methods will run and print the times in order from top to bottom, Q1 to Q10 in the terminal window.
'''


def Q_1(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ WITH ShotCounts AS (
                SELECT s.player_id, COUNT(*) AS num_shots
                FROM shot AS s
                JOIN match AS m ON s.match_id = m.match_id
                JOIN competition AS c ON m.competition_id = c.competition_id
                JOIN season AS sn ON m.season_id = sn.season_id
                WHERE s.statsbomb_xg > 0  AND c.competition_name = 'La Liga' AND sn.season_name = '2020/2021' 
                GROUP BY s.player_id
            ),
            PlayerAverages AS (
                SELECT  s.player_id, AVG(s.statsbomb_xg) AS avg_xG
                FROM shot AS s
                JOIN match AS m ON s.match_id = m.match_id
                JOIN competition AS c ON m.competition_id = c.competition_id
                JOIN season AS sn ON m.season_id = sn.season_id
                JOIN ShotCounts AS sc ON s.player_id = sc.player_id
                WHERE s.statsbomb_xg > 0  AND c.competition_name = 'La Liga'  AND sn.season_name = '2020/2021'  
                GROUP BY s.player_id
            )
            SELECT p.player_name, pa.avg_xG
            FROM player AS p
            JOIN PlayerAverages AS pa ON p.player_id = pa.player_id
            ORDER BY pa.avg_xG DESC; """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[0] = (time_val)

    write_csv(execution_time, cursor, 1)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_2(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT p.player_name, COUNT(s.shot_id) AS num_shots
            FROM player AS p
            JOIN shot AS s ON p.player_id = s.player_id
            JOIN match AS m ON s.match_id = m.match_id
            JOIN competition AS c ON m.competition_id = c.competition_id 
            JOIN season AS sn ON m.season_id = sn.season_id 
            WHERE c.competition_name = 'La Liga' AND sn.season_name = '2020/2021'
            GROUP BY p.player_name
            HAVING COUNT(s.shot_id) >= 1  
            ORDER BY num_shots DESC;   """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[1] = (time_val)

    write_csv(execution_time, cursor, 2)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_3(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT p.player_name, COUNT(s.shot_id) AS num_first_time_shots
        FROM player AS p
            JOIN shot AS s ON p.player_id = s.player_id
            JOIN match AS m ON s.match_id = m.match_id
            JOIN competition AS c ON m.competition_id = c.competition_id
            JOIN season AS sn ON m.season_id = sn.season_id
        WHERE c.competition_name = 'La Liga' 
            AND sn.season_name IN ('2020/2021', '2019/2020', '2018/2019')  
            AND s.first_time = TRUE   
        GROUP BY p.player_name
        HAVING COUNT(s.shot_id) >= 1   
        ORDER BY num_first_time_shots DESC;   
        """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[2] = (time_val)

    write_csv(execution_time, cursor, 3)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_4(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT pl.player_name, t.team_name,m.competition_id, m.season_id, COUNT(*) AS passes_received
        FROM Pass p
        JOIN Player pl ON p.recipient_id  = pl.player_id
        JOIN Lineup l on pl.player_id = l.player_id
        JOIN Team t on t.team_id = l.team_id
        JOIN Match m on p.match_id = m.match_id
        JOIN competition c ON c.competition_id = m.competition_id
        WHERE c.competition_id = 11 
            AND m.season_id = 44 
        GROUP BY pl.player_name, t.team_name, m.competition_id, m.season_id 
        HAVING  COUNT(*) > 1
        ORDER BY passes_received DESC; """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[3] = (time_val)

    write_csv(execution_time, cursor, 4)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_5(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT pl.player_name , COUNT(*) AS passes_received
            FROM Pass p
            JOIN Player pl ON p.recipient_id  = pl.player_id
            JOIN Match m on p.match_id = m.match_id
            WHERE m.competition_id = (SELECT competition_id FROM competition WHERE competition_name = 'Premier League')
            AND m.season_id = (SELECT season_id FROM season WHERE season_name = '2003/2004')
            GROUP BY pl.player_name 
            HAVING  COUNT(*) > 1
            ORDER BY passes_received DESC; """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[4] = (time_val)

    write_csv(execution_time, cursor, 5)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_6(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT t.team_name, COUNT(s.shot_id) AS num_shots
            FROM team AS t
                JOIN match AS m ON t.team_id = m.home_team_id OR t.team_id = m.away_team_id
                JOIN shot AS s ON m.match_id = s.match_id
                JOIN competition AS c ON m.competition_id = c.competition_id
                JOIN season AS sn ON m.season_id = sn.season_id
            WHERE c.competition_name = 'Premier League' 
                AND sn.season_name = '2003/2004'  
            GROUP BY t.team_name
            HAVING COUNT(s.shot_id) >= 1   
            ORDER BY num_shots DESC;   """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[5] = (time_val)

    write_csv(execution_time, cursor, 6)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_7(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT player.player_name, COUNT(pass.pass_id) AS number_of_through_balls
            FROM pass
            JOIN player ON pass.player_id = player.player_id
            JOIN match ON pass.match_id = match.match_id
            JOIN pass_technique ON pass.pass_technique_id = pass_technique.pass_technique_id
            WHERE match.competition_id = (SELECT competition_id FROM competition WHERE competition_name = 'La Liga')
            AND match.season_id = (SELECT season_id FROM season WHERE season_name = '2020/2021')
            AND pass_technique.pass_technique_name = 'Through Ball'
            GROUP BY player.player_name
            HAVING COUNT(pass.pass_id) >= 1
            ORDER BY number_of_through_balls DESC;
            """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[6] = (time_val)

    write_csv(execution_time, cursor, 7)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_8(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT team.team_name, COUNT(pass.pass_id) AS number_of_through_balls
            FROM pass
            JOIN team ON pass.team_id = team.team_id
            JOIN match ON pass.match_id = match.match_id
            JOIN pass_technique ON pass.pass_technique_id = pass_technique.pass_technique_id
            WHERE match.competition_id = (SELECT competition_id FROM competition WHERE competition_name = 'La Liga')
            AND match.season_id = (SELECT season_id FROM season WHERE season_name = '2020/2021')
            AND pass_technique.pass_technique_name = 'Through Ball'
            GROUP BY team.team_name
            HAVING COUNT(pass.pass_id) >= 1
            ORDER BY number_of_through_balls DESC; """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[7] = (time_val)

    write_csv(execution_time, cursor, 8)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_9(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT player.player_name, COUNT(dribble.dribble_id) AS number_of_successful_dribbles
            FROM dribble
            JOIN player ON dribble.player_id = player.player_id
            JOIN match ON dribble.match_id = match.match_id
            WHERE match.competition_id = (SELECT competition_id FROM competition WHERE competition_name = 'La Liga')
            AND (match.season_id IN (SELECT season_id FROM season WHERE season_name IN ('2020/2021', '2019/2020', '2018/2019')))
            AND dribble.dribble_outcome_id = (SELECT dribble_outcome_id FROM dribble_outcome WHERE dribble_outcome_name = 'Complete')
            GROUP BY player.player_name
            HAVING COUNT(dribble.dribble_id) >= 1
            ORDER BY number_of_successful_dribbles DESC; """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[8] = (time_val)

    write_csv(execution_time, cursor, 9)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_10(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    # ==========================================================================
    # Enter QUERY within the quotes:

    query = """ SELECT player.player_name, COUNT(dribbled_past.dribbled_past_id) AS number_of_times_dribbled_past
            FROM dribbled_past
            JOIN player ON dribbled_past.player_id = player.player_id
            JOIN match ON dribbled_past.match_id = match.match_id
            WHERE match.competition_id = (SELECT competition_id FROM competition WHERE competition_name = 'La Liga')
            AND match.season_id = (SELECT season_id FROM season WHERE season_name = '2020/2021')
            GROUP BY player.player_name
            HAVING COUNT(dribbled_past.dribbled_past_id) >= 1
            ORDER BY number_of_times_dribbled_past ASC; """

    # ==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[9] = (time_val)

    write_csv(execution_time, cursor, 10)

    cursor.close()
    new_conn.close()

    return reconnect()

# Running the queries from the Q_n methods - Do NOT Modify
# =====================================================


def run_queries(conn):

    execution_time = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    conn = Q_1(conn, execution_time)
    conn = Q_2(conn, execution_time)
    conn = Q_3(conn, execution_time)
    conn = Q_4(conn, execution_time)
    conn = Q_5(conn, execution_time)
    conn = Q_6(conn, execution_time)
    conn = Q_7(conn, execution_time)
    conn = Q_8(conn, execution_time)
    conn = Q_9(conn, execution_time)
    conn = Q_10(conn, execution_time)

    for i in range(10):
        print(execution_time[i])


''' MAIN '''
try:
    if __name__ == "__main__":

        dbname = root_database_name
        user = db_username
        password = db_password
        host = db_host
        port = db_port

        conn = psycopg.connect(dbname=dbname, user=user,
                               password=password, host=host, port=port)

        run_queries(conn)
except Exception as error:
    print(error)
    # print("[ERROR]: Failure to connect to database.")
# _______________________________________________________
