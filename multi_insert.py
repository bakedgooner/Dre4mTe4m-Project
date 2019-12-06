def multi_row_teams(file_name='teams.csv'):
    df = pd.read_csv(file_name, sep=',', header=None)
    cols = str(df.columns.values.tolist()).replace('[','').replace(']', '')
    values = list(map(tuple, df.values))
    db = sqlcon.connect(user='root', 
                        password='****', 
                        host='127.0.0.1', 
                        database='****', 
                        auth_plugin='mysql_native_password')
    pointer = db.cursor()
    start = time.time()
    
    sql = "insert into teams ( TeamID, TeamName, city ) values  (%s, %s, %s );" 
    try:
        pointer.executemany(sql, values)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close()
    end = time.time()
    print(end - start, 'seconds')
    s=str(end - start) + 'seconds'
    return s