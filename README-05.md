# geek
geek homework - hive


## run
**table**
```sql
create EXTERNAL table t_rating (userid int, movieid int , rate int , times  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" ="::")
STORED AS TEXTFILE
LOCATION  '/data/hive/ratings/';

create EXTERNAL table t_movie  (userid int , movieid int, rate int , times string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" ="::")
STORED AS TEXTFILE
LOCATION  '/data/hive/movies/';

create EXTERNAL table t_user  (userid int , sex string, age int , occupation int, zipcode int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim" ="::")
STORED AS TEXTFILE
LOCATION  '/data/hive/users/';

```
```sql
# Q1: 展示电影 ID 为 2116 这部电影各年龄段的平均影评分
select u.age, avg(r.rate) from t_rating r left join t_movie m left join t_user u  on r.movieid=m.movieid and r.userid=u.userid where m.movieid=2116 group by u.age;
# Q2: 找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数
select m.moviename, avg(r.rate), count(r.rate) from t_rating r left join t_movie m left join t_user u  on r.movieid=m.movieid and r.userid=u.userid where u.sex='M' group by m.moviename having count(r.rate)>50 limit 10;
# Q3: 找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分

select m.moviename, avg(r.rate) nr
from t_rating r left join t_movie m left join t_user u  on r.movieid=m.movieid and r.userid=u.userid
where u.sex='F' and r.movieid in (select c.movieid from (
select cc.movieid from (select r.movieid, count(r.rate) ct from t_rating r group by r.movieid ) cc order by cc.ct desc limit 10
) c) group by m.moviename;
```
