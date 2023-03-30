--- Топ 5 найпопулярніших статей
SELECT COUNT(ds.source), ds.target
FROM (SELECT DISTINCT(source), target FROM wikiracer)
AS ds
GROUP BY target
ORDER BY count
DESC LIMIT 5;

--- Топ 5 статей з найбільшою кількістю посилань на інші статті
SELECT COUNT(source), source
FROM wikiracer
GROUP BY source
ORDER BY count
DESC LIMIT 5;

--- Середня кількість нащадків другого рівня
SELECT COUNT(target) / COUNT(distinct(source))
FROM wikiracer
WHERE source
IN (SELECT DISTINCT target FROM wikiracer WHERE source = 'Дружба')
AND target != 'Дружба';

SELECT count(DISTINCT(target)) FROM wikiracer WHERE source = 'Дружба (значення)';
SELECT target, source FROM wikiracer WHERE source IN (SELECT DISTINCT target FROM wikiracer WHERE source = 'Дружба') AND target != 'Дружба';
select distinct(source), count(target) from wikiracer group by source;
select distinct(source), count(target) as c, from wikiracer group by source;
-----
select avg(c.target_count) from (select distinct(source), count(target) as target_count from wikiracer group by source) as c;