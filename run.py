from mysql.connector import connect
import error
import csv
import json
import os

'''
schema

DVD (d_id, d_title, d_name, d_state) 
USERS (u_id, u_name, u_age, u_state)
REVIEW (d_id, u_id, rating)
RENT_STATE (r_id, d_id, u_id)

'''

FILE_PATH = "director_movies.json" 
director_movies = {} # director name을 저장하는 dict (삭제된 dvd의 감독명도 저장하고 있음)

connection = connect(
    host = 'astronaut.snu.ac.kr',
    port = 7000,
    user = 'DB2023_19827',
    password = 'DB2023_19827',
    db = 'DB2023_19827',
    charset = 'utf-8'
)

cursor = connection.cursor(dictionary=True)

def save_data():
    with open(FILE_PATH, "w") as f:
        json.dump(director_movies, f)

def load_data():
    global director_movies
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r") as f:
            director_movies = json.load(f)

def initialize_database():
    # DB 초기화
    cursor.execute('''
                   CREATE TABLE dvd(
                    d_id INT AUTO_INCREMENT NOT NULL, 
                    d_title CHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, 
                    d_name CHAR(50), 
                    d_state INT DEFAULT 2, 
                    PRIMARY KEY (d_id), 
                    UNIQUE (d_title, d_name))
                   ''')
    cursor.execute('''
                   CREATE TABLE users(
                    u_id INT AUTO_INCREMENT NOT NULL, 
                    u_name CHAR(50), 
                    u_age INT, 
                    u_state INT DEFAULT 3,
                    PRIMARY KEY (u_id))
                   ''')
    cursor.execute('''
                   CREATE TABLE review(
                    d_id INT NOT NULL, 
                    u_id INT NOT NULL, 
                    rating INT)
                   ''')
    cursor.execute('''
                   CREATE TABLE rent_state(
                    r_id INT AUTO_INCREMENT NOT NULL,
                    d_id INT NOT NULL,
                    u_id INT NOT NULL,
                    PRIMARY KEY (r_id),
                    FOREIGN KEY (d_id) REFERENCES dvd (d_id),
                    FOREIGN KEY (u_id) REFERENCES users (u_id))
                   ''')

    # data.csv 처리기
    dvd = {}
    user = {}
    review = []

    with open('data.csv', mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # 헤더 스킵
        for row in reader:
            dvd[row[1]] = (row[2], row[3])
            user[row[4]] = (row[5], int(row[6]))
            review.append((int(row[1]), int(row[4]), int(row[7])))

    # DVD 데이터 삽입
    cursor.executemany(
        "INSERT INTO dvd (d_id, d_title, d_name, d_state) VALUES (%s, %s, %s, %s)",
        [(int(d_id), d_title, d_name, 2) for d_id, (d_title, d_name) in dvd.items()]
    )

    # d_name은 따로 저장
    for d_id, (d_title, d_name) in dvd.items():
        director_movies[d_name] = []

    # 사용자 데이터 삽입
    cursor.executemany(
        "INSERT INTO users (u_id, u_name, u_age, u_state) VALUES (%s, %s, %s, %s)",
        [(int(u_id), u_name, u_age, 3) for u_id, (u_name, u_age) in user.items()]
    )

    # 리뷰 데이터 삽입
    cursor.executemany(
        "INSERT INTO review (d_id, u_id, rating) VALUES (%s, %s, %s)",
        review
    )

    # commit 후 메시지 출력
    connection.commit()
    print('Database successfully initialized')

def reset():
    messege = input("Are you going to reset the database? (y/n)")
    if messege == "y":
        global director_movies
        director_movies = {}

        cursor.execute("DELETE FROM review")        
        cursor.execute("DELETE FROM rent_state")
        cursor.execute("DELETE FROM users")
        cursor.execute("DELETE FROM dvd")

        cursor.execute("DROP TABLE review")
        cursor.execute("DROP TABLE rent_state")
        cursor.execute("DROP TABLE users")
        cursor.execute("DROP TABLE dvd")

        connection.commit()

        initialize_database()
        
def print_output(data):
    '''
    output 규격을 설정하여 print하는 함수

    Arg :
        data = [{column_name1 : value1, ...}, ...]

    Return : 
        None
    '''

    fields_definition_list = list(data[0].keys()) #table의 column_name 불러오기
    record_list = [[] for _ in range(len(fields_definition_list))] #table의 column 개수만큼 list제작
    
    # 비어있는 table 출력
    if data[0][fields_definition_list[0]] == None:
        column_widths = [len(fields_definition_list[i]) for i in range(len(fields_definition_list))]
        print("".join("-" * (i+5) for i in column_widths))
        print("|".join(fields_definition_list[i].ljust(column_widths[i]+4, " ") for i in range(len(fields_definition_list))))
        print("".join("-" * (i+5) for i in column_widths))
        return

    # recordes를 순회하면서 record_list에 field별 원소 넣기
    for recorde in data:
        for idx in range(len(fields_definition_list)):
            record_list[idx].append(recorde[fields_definition_list[idx]])

    #view 제작
    column_widths = [
        max(len(fields_definition_list[i]),
            max(len(str(record_list[i][j])) for j in range(len(record_list[i])))) for i in range(len(fields_definition_list))
        ]
    print("".join("-" * (i+5) for i in column_widths))
    print("|".join(fields_definition_list[i].ljust(column_widths[i]+4, " ") for i in range(len(fields_definition_list))))
    print("".join("-" * (i+5) for i in column_widths))
    for i in range(len(record_list[0])):
            print("|".join(str(record_list[j][i]).ljust(column_widths[j]+4, " ") for j in range(len(record_list))))
    print("".join("-" * (i+5) for i in column_widths)) 
    
def print_DVDs():
    '''
    print 형식 : DVD ID, 영화 제목, 감독명, 평균 평점, 누적 대출 횟수, 대출 가능 수
    '''
    cursor.execute('''
            SELECT r.id, r.title, r.director, ROUND(r.avg_rating, 3) AS avg_rating, r.cumul_rent_cnt AS cumul_rent_cnt, r.quantity
            FROM (SELECT dvd.d_id AS id, dvd.d_title AS title, dvd.d_name AS director, AVG(rating) AS avg_rating, COUNT(review.d_id) AS cumul_rent_cnt, d_state AS quantity
                FROM dvd
                LEFT OUTER JOIN review ON dvd.d_id = review.d_id
                GROUP BY dvd.d_id, dvd.d_title, d_name, d_state
                ORDER BY avg_rating DESC, cumul_rent_cnt DESC) AS r
            ''')
    output = cursor.fetchall()

    for i in range(len(output)):
        if output[i]["cumul_rent_cnt"] == 'null':
            output[i]["cumul_rent_cnt"] = 0
            output[i]["avg_rating"] = 'None'

    print_output(output)

def print_users():
    '''
    print 형식 : 회원ID, 회원명, 회원 나이, 대출하고 반납한 DVD들에 대한 평균 평점, 누적 대출 횟수
    '''
    cursor.execute('''
            select users.u_id AS id, users.u_name AS name, users.u_age AS age, ROUND(AVG(rating), 3) AS avg_rating, COUNT(review.u_id) AS cumul_rent_cnt
            from users 
            left outer join review on users.u_id = review.u_id
            group by users.u_id, users.u_name, users.u_age
            ORDER BY id
            ''')
    output = cursor.fetchall()

    for i in range(len(output)):
        if output[i]["cumul_rent_cnt"] == 'null':
            output[i]["cumul_rent_cnt"] = 0
            output[i]["avg_rating"] = 'None'

    print_output(output)

def insert_DVD():
    title = input('DVD title: ')
    director = input('DVD director: ')


    
    if len(title) > 100 or len(title) < 1:
        raise error.E1()


    if len(director) > 50 or len(title) < 1:
        raise error.E2()

    
    
    pair = (title, director)

    cursor.execute('''
                    SELECT COUNT(*) FROM dvd WHERE d_title = %s and d_name = %s
                   ''',
                   pair)
    result = cursor.fetchall()

    if result[0]['COUNT(*)'] != 0:
        raise error.E3(title, director)

    cursor.execute('''
                    INSERT INTO dvd (d_title, d_name) VALUES (%s, %s)
                   ''',
                   pair)
    
    connection.commit()

    director_movies[director] = [] # 감독 dict에 추가

    print("DVD successfully added")

def remove_DVD():
    DVD_id = [input('DVD ID: ')]
    
    cursor.execute('''
                SELECT COUNT(*)
                FROM dvd
                WHERE d_id = %s
                ''',
                DVD_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] == 0:
        raise error.E5(DVD_id[0])
    
    cursor.execute('''
                SELECT COUNT(*)
                FROM rent_state
                WHERE d_id = %s
                ''',
                DVD_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] != 0:
        raise error.E6()
    
    cursor.execute('''
                DELETE FROM dvd WHERE d_id = %s
                ''',
                DVD_id)
    
    connection.commit()
    print("DVD successfully removed")

def insert_user():
    name = input('User name: ')
    age = input('User age: ')
    


    if len(name) > 50 or len(name) < 1:
        raise error.E4()

    if is_positive_integer(age) == False:
        raise error.E14()
    
    pair = (name, age)

    cursor.execute('''
                    SELECT COUNT(*) FROM users WHERE u_name = %s and u_age = %s
                   ''',
                   pair)
    result = cursor.fetchall()
    if result[0]['COUNT(*)'] != 0:
        raise error.E13(name, age)
    
    cursor.execute('''
                    INSERT INTO users (u_name, u_age) VALUES (%s, %s)
                   ''',
                   pair)
    connection.commit()
    print("User successfully added")

def is_positive_integer(value):
    '''
    양의 정수인지 확인하는 함수

    Arg : 
        value = any type 

    return : 
        Bool
    '''
    try:
        value = int(value)
        return value > 0
    except ValueError:
        return False
    
def remove_user():
    user_id = [input('User ID: ')]
    
    cursor.execute('''
                   SELECT COUNT(*)
                   FROM users
                   WHERE u_id = %s
                   ''', user_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] == 0:
        raise error.E7(user_id[0])
    
    cursor.execute('''
                SELECT COUNT(*)
                FROM rent_state
                WHERE u_id = %s
                ''',
                user_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] != 0:
        raise error.E8()
    
    cursor.execute('''
                DELETE FROM users WHERE u_id = %s
                ''',
                user_id)
    
    connection.commit()
    print("User successfully removed")
    
def checkout_DVD():
    DVD_id = [input('DVD ID: ')]
    user_id = [input('User ID: ')]
    pair = [DVD_id[0], user_id[0]]

    cursor.execute('''
                   SELECT COUNT(*)
                   FROM dvd
                   WHERE d_id = %s
                   ''', DVD_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] == 0:
        raise error.E5(DVD_id[0])

    
    cursor.execute('''
                   SELECT COUNT(*)
                   FROM users
                   WHERE u_id = %s
                   ''', user_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] == 0:
        raise error.E7(user_id[0])

    cursor.execute('''
                   SELECT u_state
                   FROM users
                   WHERE u_id = %s
                   ''', user_id)
    result = cursor.fetchall()
    if result[0]["u_state"] == 0:
        raise error.E10(user_id[0])
    
    cursor.execute('''
                   SELECT d_state
                   FROM dvd
                   WHERE d_id = %s
                   ''', DVD_id)
    result = cursor.fetchall()
    if result[0]["d_state"] == 0:
        raise error.E9()
    
    cursor.execute('''
                   SELECT COUNT(*)
                   FROM rent_state
                   WHERE d_id = %s and u_id = %s
                   ''', pair)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] != 0:
        raise error.E15()
    
    cursor.execute("INSERT INTO rent_state (d_id, u_id) VALUES (%s, %s)", pair)
    cursor.execute("UPDATE dvd SET d_state = d_state - 1 WHERE d_id = %s", DVD_id)
    cursor.execute("UPDATE users SET u_state = u_state - 1 WHERE u_id = %s", user_id)
    connection.commit()
    print("DVD successfully checked out")

def return_and_rate_DVD():
    DVD_id = [input('DVD ID: ')]
    user_id = [input('User ID: ')]
    rating = [input('Ratings (1~5): ')]

    cursor.execute('''
                   SELECT COUNT(*)
                   FROM dvd
                   WHERE d_id = %s
                   ''', DVD_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] == 0:
        raise error.E5(DVD_id[0])

    cursor.execute('''
                   SELECT COUNT(*)
                   FROM users
                   WHERE u_id = %s
                   ''', user_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] == 0:
        raise error.E7(user_id[0])
    
    valid_rating = False
    if is_positive_integer(rating[0]):
        r_val = int(rating[0])
        if 1 <= r_val <= 5:
            valid_rating = True
    if not valid_rating:
        raise error.E11()
    
    pair = [DVD_id[0], user_id[0]]
    cursor.execute('''
                   SELECT COUNT(*)
                   FROM rent_state
                   WHERE d_id = %s and u_id = %s
                   ''', pair)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] == 0:
        raise error.E12()
    
    cursor.execute("DELETE FROM rent_state WHERE d_id = %s and u_id = %s", pair)
    cursor.execute("UPDATE dvd SET d_state = d_state + 1 WHERE d_id = %s", DVD_id)
    cursor.execute("UPDATE users SET u_state = u_state + 1 WHERE u_id = %s", user_id)
    pair.append(rating[0])
    cursor.execute("INSERT INTO review (d_id, u_id, rating) VALUES (%s, %s, %s)", pair)

    connection.commit()
    print("DVD successfully returned and rated")

def print_borrowing_status_for_user():
    user_id = [input('User ID: ')]

    cursor.execute('''
                   SELECT COUNT(*)
                   FROM users
                   WHERE u_id = %s
                   ''', user_id)
    result = cursor.fetchall()
    if result[0]["COUNT(*)"] == 0:
        raise error.E7(user_id[0])
    
    query = '''
        SELECT dvd.d_id AS id, dvd.d_title AS title, dvd.d_name AS director, ROUND(AVG(rating), 3) AS avg_rating
        FROM dvd 
        JOIN (SELECT d_id FROM rent_state WHERE u_id = %s) AS rented USING (d_id)
        LEFT OUTER JOIN review USING (d_id)
        GROUP BY dvd.d_id, dvd.d_title, dvd.d_name
    '''
    cursor.execute(query, user_id)
    result = cursor.fetchall()
    if result == []:
        result = [{"id" : None, "title" : None, "director" : None, "avg_rating" : None}]
    print_output(result)

def search_dvd():
    '''
    DVD ID, 영화 제목, 감독명, 평균 평점, 누적 대출 횟수, 대출가능 수 
    '''
    query = input('Query: ')
    
    cursor.execute('''
            SELECT r.id, r.title, r.director, ROUND(r.avg_rating, 3) AS avg_rating, r.cumul_rent_cnt AS cumul_rent_cnt, r.quantity
            FROM (SELECT dvd.d_id AS id, dvd.d_title AS title, dvd.d_name AS director, AVG(rating) AS avg_rating, COUNT(review.d_id) AS cumul_rent_cnt, d_state AS quantity
                FROM dvd
                LEFT OUTER JOIN review ON dvd.d_id = review.d_id
                GROUP BY dvd.d_id, dvd.d_title, d_name, d_state
                ORDER BY avg_rating DESC, cumul_rent_cnt DESC) AS r
            ''')
    
    result = cursor.fetchall()
    output = []
    for idx in range(len(result)):
        title = result[idx]["title"]
        if query.upper() in title.upper():
            output.append(result[idx])

    if output == []:
        raise error.E16()
    
    for i in range(len(output)):
        if output[i]["cumul_rent_cnt"] == 'null':
            output[i]["cumul_rent_cnt"] = 0
            output[i]["avg_rating"] = 'None'
    print_output(output)

def search_director():
    '''
    감독명, 감독 평점, 누적 대출 횟수, 영화제목(list 형태)
    '''
    query = input('Query: ')
    
    cursor.execute('''
            SELECT d.director, ROUND(d.avg_rating, 3) AS avg_rating, d.cumul_rent_cnt, d.titles
            FROM(SELECT i.director, AVG(i.avg_rating) AS avg_rating, SUM(i.cumul_rent_cnt) AS cumul_rent_cnt, GROUP_CONCAT(i.title SEPARATOR '|') AS titles
                FROM (SELECT r.id, r.title, r.director, ROUND(r.avg_rating, 3) AS avg_rating, r.cumul_rent_cnt + (2-r.quantity) AS cumul_rent_cnt, r.quantity
                    FROM (SELECT dvd.d_id AS id, dvd.d_title AS title, dvd.d_name AS director, AVG(rating) AS avg_rating, COUNT(review.d_id) AS cumul_rent_cnt, d_state AS quantity
                        FROM dvd
                        LEFT OUTER JOIN review ON dvd.d_id = review.d_id
                        GROUP BY dvd.d_id, dvd.d_title, d_name, d_state) AS r) AS i
                GROUP BY i.director
                ORDER BY avg_rating DESC, cumul_rent_cnt DESC) AS d
            ''')
    
    result = cursor.fetchall()
    output = []
    directors = {}

    for idx in range(len(result)):
        director = result[idx]["director"]
        if query.upper() in director.upper():
            output.append(result[idx])
            directors[director] = [] 
    
    dir_names = list(director_movies.keys())
    for idx in range(len(dir_names)):
        dir_name = dir_names[idx]
        if query.upper() in dir_name.upper():
            if dir_name in list(directors.keys()):
                continue
            output.append({"director" : dir_name,
                            "avg_rating" : 'None',
                            "cumul_rent_cnt" : 'null',
                            "titles" : '[]'})

    if output == []:
        raise error.E16()
    
    for idx in range(len(output)):
        titles = output[idx]['titles'].split('|')
        titles.sort()
        txt = "[" + ', '.join(titles) + "]"
        output[idx]['titles'] = txt
        if output[idx]["cumul_rent_cnt"] == 'null':
            output[idx]["cumul_rent_cnt"] = 0
            output[idx]["avg_rating"] = 'None'

    print_output(output)

def recommend_popularity():
    user_id = [input('User ID: ')]

    # 사용자 존재 여부 확인
    cursor.execute('''
                   SELECT COUNT(*) AS cnt
                   FROM users
                   WHERE u_id = %s
                   ''', user_id)
    result = cursor.fetchall()
    if result[0]["cnt"] == 0:
        raise error.E7(user_id[0])

    # 평균 평점 기준 추천 DVD
    cursor.execute('''
        SELECT t.d_id, t.title, t.director, ROUND(t.avg_rating, 3) AS avg_rating, t.cumul_rent_cnt, t.quantity
        FROM (
            SELECT dvd.d_id,
                   dvd.d_title AS title,
                   dvd.d_name AS director,
                   AVG(review.rating) AS avg_rating,
                   COUNT(review.rating) AS cumul_rent_cnt,
                   dvd.d_state AS quantity,
                   (
                     SELECT AVG(r2.rating)
                     FROM dvd d2
                     JOIN review r2 ON d2.d_id = r2.d_id
                     WHERE d2.d_name = dvd.d_name
                   ) AS director_avg_rating
            FROM dvd
            LEFT JOIN review ON dvd.d_id = review.d_id
            WHERE dvd.d_id NOT IN (
                SELECT d_id FROM review WHERE u_id = %s
            )
            GROUP BY dvd.d_id, dvd.d_title, dvd.d_name, dvd.d_state
        ) AS t
        ORDER BY 
            t.avg_rating DESC,
            t.cumul_rent_cnt DESC,
            director_avg_rating DESC,
            t.d_id ASC
        LIMIT 1
    ''', user_id)
    top_avg_result = cursor.fetchall()

    # 누적 대출 횟수 기준 추천 DVD
    cursor.execute('''
        SELECT t.d_id, t.title, t.director, ROUND(t.avg_rating, 3) AS avg_rating, t.cumul_rent_cnt, t.quantity
        FROM (
            SELECT dvd.d_id,
                   dvd.d_title AS title,
                   dvd.d_name AS director,
                   AVG(review.rating) AS avg_rating,
                   COUNT(review.rating) AS cumul_rent_cnt,
                   dvd.d_state AS quantity,
                   (
                     SELECT AVG(r2.rating)
                     FROM dvd d2
                     JOIN review r2 ON d2.d_id = r2.d_id
                     WHERE d2.d_name = dvd.d_name
                   ) AS director_avg_rating
            FROM dvd
            LEFT JOIN review ON dvd.d_id = review.d_id
            WHERE dvd.d_id NOT IN (
                SELECT d_id FROM review WHERE u_id = %s
            )
            GROUP BY dvd.d_id, dvd.d_title, dvd.d_name, dvd.d_state
        ) AS t
        ORDER BY 
            t.cumul_rent_cnt DESC,
            t.avg_rating DESC,
            director_avg_rating DESC,
            t.d_id ASC
        LIMIT 1
    ''', user_id)
    top_cumul_result = cursor.fetchall()

    # 결과가 없는 경우 처리
    if len(top_avg_result) == 0:
        top_avg_result = [{"d_id": None, "title": None, "director": None, "avg_rating": None, "quantity": None}]
    if len(top_cumul_result) == 0:
        top_cumul_result = [{"d_id": None, "title": None, "director": None, "cumul_rent_cnt": None, "quantity": None}]

    # Rating-based
    rating_based_data = [{
        "id": r["d_id"],
        "title": r["title"],
        "director": r["director"],
        "avg_rating": r["avg_rating"], 
        "quantity": r["quantity"]
    } for r in top_avg_result]

    # Popularity-based
    popularity_based_data = [{
        "id": r["d_id"],
        "title": r["title"],
        "director": r["director"],
        "cumul_rent_cnt": r["cumul_rent_cnt"],  
        "quantity": r["quantity"]
    } for r in top_cumul_result]

    # Rating-based 출력
    print("Rating-based")
    print_output(rating_based_data)
    print("")

    # Popularity-based 출력
    print("Popularity-based")
    print_output(popularity_based_data)
    print("")

def recommend_user_based():
    user_id = [input('User ID: ')]
    
    cursor.execute('''
                   SELECT COUNT(*) AS cnt 
                   FROM users 
                   WHERE u_id = %s'''
                   , user_id)
    result = cursor.fetchall()
    if result[0]['cnt'] == 0:
        raise error.E7(user_id[0])
    u = int(user_id[0])  

    # user-item matrix 구축
    cursor.execute('SELECT u_id, d_id, rating FROM review')
    reviews = cursor.fetchall()

    # user_ratings[u_id] = {d_id: rating, ...}
    user_ratings = {}
    dvd_set = set()
    user_set = set()
    for r in reviews:
        uid = r['u_id']
        did = r['d_id']
        rt = r['rating']
        if uid not in user_ratings:
            user_ratings[uid] = {}
        user_ratings[uid][did] = rt
        dvd_set.add(did)
        user_set.add(uid)

    # 목표 사용자가 평점 남긴 DVD 집합
    rated_by_u = set(user_ratings[u].keys()) if u in user_ratings else set()

    # 후보 DVD: u가 평점을 남기지 않은 DVD
    candidate_dvds = [d for d in dvd_set if d not in rated_by_u]

    # 후보가 없으면 사용자에게 평점을 남길 DVD가 없는 경우 처리
    if len(candidate_dvds) == 0:
        # None 출력 처리
        final_output = [{"id": None, "title": None, "director": None, "avg.rating": None, "exp.rating": None}]
        print_output(final_output)
        return

    # 사용자별 평균 평점 r̄_u 계산
    user_avg = {}
    for uid in user_ratings:
        ratings_list = list(user_ratings[uid].values())
        user_avg[uid] = sum(ratings_list) / len(ratings_list)

    # 목표 사용자의 평균 평점 r̄_u
    r_u_bar = user_avg[u] if u in user_avg else 0.0

    # 유사도 계산 (s(u,v)) : 모든 v != u에 대해
    def pearson(u, v):
        if u not in user_ratings or v not in user_ratings:
            return 0
        common_items = set(user_ratings[u].keys()) & set(user_ratings[v].keys())
        if len(common_items) == 0:
            return 0
        numerator = 0.0
        denom_left = 0.0
        denom_right = 0.0
        for i in common_items:
            r_u_i = user_ratings[u][i]
            r_v_i = user_ratings[v][i]
            numerator += (r_u_i - user_avg[u]) * (r_v_i - user_avg[v])
            denom_left += (r_u_i - user_avg[u])**2
            denom_right += (r_v_i - user_avg[v])**2
        if denom_left == 0 or denom_right == 0:
            return 0
        return numerator / ((denom_left**0.5)*(denom_right**0.5))

    # u와 다른 사용자와의 유사도 저장
    similarities = {}
    for v in user_set:
        if v != u:
            similarities[v] = pearson(u, v)

    # 각 후보 DVD i에 대해 예상 평점 계산
    predictions = []
    for i in candidate_dvds:
        numerator = 0.0
        denominator = 0.0
        for v in similarities:
            s_uv = similarities[v]
            if s_uv != 0 and v in user_ratings and i in user_ratings[v]:
                numerator += s_uv * (user_ratings[v][i] - user_avg[v])
                denominator += abs(s_uv)
        if denominator == 0:
            r_hat = r_u_bar
        else:
            r_hat = r_u_bar + (numerator / denominator)
        
        predictions.append((i, r_hat))

    # 가장 높은 예상 평점의 DVD 선택 : 예상 평점이 같으면 DVD id가 가장 작은 것
    predictions.sort(key=lambda x: (-x[1], x[0]))  
    best_dvd_id, best_pred = predictions[0]

    cursor.execute('''
        SELECT dvd.d_id AS id, dvd.d_title AS title, dvd.d_name AS director, ROUND(AVG(review.rating), 3) AS avg_rating
        FROM dvd
        LEFT JOIN review ON dvd.d_id = review.d_id
        WHERE dvd.d_id = %s
        GROUP BY dvd.d_id, dvd.d_title, dvd.d_name
    ''', [best_dvd_id])
    dvd_info = cursor.fetchall()
    if len(dvd_info) == 0:
        # DVD 정보 없는 경우 처리
        final_output = [{"id": None, "title": None, "director": None, "avg.rating": None, "exp.rating": None}]
        print_output(final_output)
        return

    final_output = [{
        "id": dvd_info[0]["id"],
        "title": dvd_info[0]["title"],
        "director": dvd_info[0]["director"],
        "avg_rating": dvd_info[0]["avg_rating"] if dvd_info[0]["avg_rating"] is not None else 'None',
        "exp_rating": round(best_pred, 3)
    }]

    print_output(final_output)

def main():

    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all DVDs')
        print('3. print all users')
        print('4. insert a new DVD')
        print('5. remove a DVD')
        print('6. insert a new user')
        print('7. remove a user')
        print('8. check out a DVD')
        print('9. return and rate a DVD')
        print('10. print borrowing status of a user')
        print('11. search DVDs')
        print('12. search directors')
        print('13. recommend a DVD for a user using popularity-based method')
        print('14. recommend a DVD for a user using user-based collaborative filtering')
        print('15. exit')
        print('16. reset database')
        print('============================================================')
        try:
            menu = int(input('Select your action: '))

            if menu == 1:
                initialize_database()
            elif menu == 2:
                print_DVDs()
            elif menu == 3:
                print_users()
            elif menu == 4:
                insert_DVD()
            elif menu == 5:
                remove_DVD()
            elif menu == 6:
                insert_user()
            elif menu == 7:
                remove_user()
            elif menu == 8:
                checkout_DVD()
            elif menu == 9:
                return_and_rate_DVD()
            elif menu == 10:
                print_borrowing_status_for_user()
            elif menu == 11:
                search_dvd()
            elif menu == 12:
                search_director()
            elif menu == 13:
                recommend_popularity()
            elif menu == 14:
                recommend_user_based()
            elif menu == 15:
                print('Bye!')
                break
            elif menu == 16:
                reset()
            else:
                print('Invalid action')
        except Exception as e:
            print(e)

if __name__ == "__main__":
    load_data()
    main()
    save_data()
    connection.close()
