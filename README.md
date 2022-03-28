# geek
geek homework - hive


## run
```sql
# Q1
select u.age, avg(r.rate) from t_rating r left join t_movie m left join t_user u  on r.movieid=m.movieid and r.userid=u.userid group by u.age;
# Q2
select m.moviename, avg(r.rate), count(r.userid) from t_movie m left join t_rating r left join t_user u  on r.movieid=m.movieid and r.userid=u.userid where u.sex='男' group by m.moviename having count(r.userid)>50 limit 10;
# Q3

from (
select m.movieid from (select m.movieid, count(r.rate) ct from t_movie m left join t_rating r on m.movieid = r.movieid group by m.movieid ) c order by c.ct desc limit 10
) c
select w.nr, w.moviename, w2.ar from (
                                         select avg(r.rate) nr, m.moviename
                                         from t_movie m left join t_rating r on m.movieid = r.movieid
                                         where u.sex='女' and m.movieid in (select c.movieid from c)
                                     ) w left join (
    select avg(r.rate) ar, m.moviename
    from t_movie m left join t_rating r on m.movieid = r.movieid
    where m.movieid in (select c.movieid from c)
) w2 on w.moviename = w2.moviename;
```

## report
***Q1***
![报告](https://github.com/AlitaIcon/geek/blob/main/q1.png)
***Q2***
![报告](https://github.com/AlitaIcon/geek/blob/main/q2.png)