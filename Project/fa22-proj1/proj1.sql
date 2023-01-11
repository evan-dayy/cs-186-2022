-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people 
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people 
  WHERE namefirst LIKE "% %"
  ORDER BY namefirst, namelast
  
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, avgheight, count
  FROM (
	SELECT birthYear, ROUND(AVG(height), 4) AS avgheight, COUNT(*) as count
    FROM people
    GROUP BY birthYear
    ) a
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, avgheight, count
  FROM q1iii
  WHERE avgheight > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT p.namefirst, p.namelast, p.playerID, h.yearID
  from people p, halloffame h
  WHERE p.playerID = h.playerID and h.inducted = "Y"
  ORDER BY h.yearid DESC, p.playerid
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT q.namefirst, q.namelast, q.playerid, s.schoolid, q.yearid
  FROM q2i q 
  LEFT JOIN collegeplaying c ON q.playerid = c.playerid
  LEFT JOIN schools s ON c.schoolID = s.schoolID
  WHERE s.schoolState = "CA"
  ORDER BY q.yearid DESC, s.schoolID, q.playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT DISTINCT q0.playerid, q0.namefirst, q0.namelast, q1.schoolid
  FROM q2i q0
  LEFT JOIN 
   (
	  SELECT q.namefirst, q.namelast, q.playerid as playerid, c.schoolid as schoolid, q.yearid
	  FROM q2i q 
	  inner JOIN collegeplaying c ON q.playerid = c.playerid
   )q1 ON q0.playerid = q1.playerid
  ORDER BY q0.playerid DESC, q1.schoolID
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, a.yearid, round(d1 * 1.0 / d2, 4) slg
  from people p, (
	SELECT playerid, yearid, SUM(H) + 1 * SUM(H2B) + 2 * SUM(H3B) + 3 * SUM(HR) d1, SUM(AB) d2
    FROM batting
    GROUP BY playerid, yearid, teamID
    HAVING SUM(AB) > 50
    ) a
  WHERE p.playerid = a.playerid and slg > 0
  ORDER BY slg DESC, a.yearid, p.playerid
  limit 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, round(d1 * 1.0 / d2, 4) lslg
  from people p, (
	SELECT playerid, SUM(H) + 1 * SUM(H2B) + 2 * SUM(H3B) + 3 * SUM(HR) d1, SUM(AB) d2
    FROM batting
    GROUP BY playerid
    HAVING SUM(AB) > 50
    ) a
  WHERE p.playerid = a.playerid and lslg > 0
  ORDER BY lslg DESC, p.playerid
  limit 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT p.namefirst, p.namelast, round(a.d1 * 1.0 / a.d2, 4) lslg
  from people p, (
	SELECT playerid, SUM(H) + 1 * SUM(H2B) + 2 * SUM(H3B) + 3 * SUM(HR) d1, SUM(AB) d2
    FROM batting
    GROUP BY playerid
    HAVING SUM(AB) > 50
    ) a, 
    (
	SELECT SUM(H) + 1 * SUM(H2B) + 2 * SUM(H3B) + 3 * SUM(HR) d1, SUM(AB) d2
    FROM batting
    WHERE playerid = "mayswi01"
    ) b 
    
  WHERE p.playerid = a.playerid and lslg > 0 and lslg > round(b.d1 * 1.0 / b.d2, 4)
  ORDER BY lslg DESC, p.playerid
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearid, min(salary), max(salary), avg(salary)
  from salaries
  group by yearid
  order by yearid
;


-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  SELECT 1, 1, 1, 1 -- replace this line
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  -- get the stats that I want: table t
  WITH
  t AS (
    SELECT s.yearid, MIN(s.salary) min, MAX(s.salary) max, AVG(s.salary) avg
    FROM salaries AS s
    GROUP BY s.yearid
  ),  -- don't forget the comma

  -- t1.yearsid = yearsid + 1
  t1 AS (
      SELECT s.yearid + 1 as yearid, MIN(s.salary) min, MAX(s.salary) max, AVG(s.salary) avg
      FROM salaries AS s
      GROUP BY yearid
     )

     -- join t, t1 and I have #s for both yearid and yearid + 1 to work with
     SELECT t.yearid, t.min - t1.min AS mindiff,
                      t.max - t1.max AS maxdiff,
                      t.avg - t1.avg AS avgdiff
     FROM t1 INNER JOIN t ON t1.yearid = t.yearid
     ORDER BY t1.yearid ASC
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
	WITH t AS (
	  SELECT s.yearid, MAX(s.salary) max
	  FROM salaries AS s
	  WHERE s.yearid = 2000 OR s.yearid = 2001
	  GROUP BY s.yearid
	)

	SELECT s.playerid, p.namefirst, p.namelast, s.salary, s.yearid
	FROM people AS p
		  INNER JOIN salaries AS s
			ON p.playerid = s.playerid
		  INNER JOIN t
			ON s.salary = t.max AND s.yearid = t.yearid
	;

-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
	SELECT a.teamid as team, MAX(s.salary) - MIN(s.salary) AS diffAvg
	  FROM allstarfull AS a
	  JOIN salaries AS s on s.playerid = a.playerid
	  WHERE a.yearid = 2016 and s.yearid = 2016
	  GROUP BY a.teamid
	  ORDER BY a.teamid
	;

