def bulk_teams(file_name='teams.csv'):
    db = sqlcon.connect(user='root', 
                        password='***', 
                        host='127.0.0.1', 
                        database='***', 
                        auth_plugin='mysql_native_password',
                        allow_local_infile=True)
    pointer = db.cursor()
    start = time.time()
    sql = "LOAD DATA LOCAL INFILE \'%s\' INTO TABLE teams FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' (TeamID, TeamName, city)" % (file_name)
    print(sql)
    try:
        pointer.execute(sql)
    except sqlcon.Error as e:
        return e.msg
    db.commit()
    db.close
    end = time.time()
    print(end - start, 'seconds')
    s=str(end - start) + 'seconds'
    return s