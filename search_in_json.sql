-- посмотреть что прокачано у игроков которые проходят 50 волну
DROP TABLE ws.idle_upgrades;
-- список уникальных апгрейдов
CREATE TABLE ws.idle_upgrades as
SELECT jsonb_object_keys(event_data->'State'->'WorkshopUpgrades') AS upgrade_id
FROM raw.idle_user_events
WHERE date_at >'2022-08-01'
	AND event_type = 'levelStart'
GROUP BY 1
ORDER BY 1;
-- запрос 
WITH u AS (		-- игроки
	SELECT user_id
	FROM agg.users_idle
	WHERE (progress->>'wave_num_max')::int > 50 	-- прогресс
		AND last_dt::date > '2022-10-01'
		AND reg_dt::date >= '2022-05-01'
	LIMIT 100
)
, first_win AS ( -- дата первого прохождения волны
	SELECT u.user_id
		, min(setver_dt) AS min_date
	FROM u LEFT JOIN raw.idle_user_events r using(user_id)
	WHERE date_at >'2022-05-01'
		AND event_type = 'levelFinish'
		AND (event_data ->>'waveNum')::int > 50
	GROUP BY 1
)
, u_upgardes AS ( -- кросс для списка апгрейдов
	SELECT user_id, 
			upgrade_id,
			min_date
	FROM first_win 
		CROSS JOIN ws.idle_upgrades
	)
, fin AS (		  -- получаю уровни апгрейда
	SELECT 
		u.user_id
		, u.upgrade_id
		, max(COALESCE((event_data->'State'->'WorkshopUpgrades'->>upgrade_id)::int,0)) AS upgrade_lvl
	FROM u_upgardes u
		LEFT JOIN raw.idle_user_events iue using(user_id)
	WHERE date_at >'2022-05-01'
		AND event_type = 'levelFinish'
		AND server_dt = min_date
	GROUP BY 1,2)
-- подсчет медианного и среднего уровня
SELECT count(DISTINCT user_id) AS users
	, upgrade_id
	, percentile_cont(0.5) WITHIN GROUP (ORDER BY upgrade_lvl) AS upgrade_lvl_median
	, round(avg(upgrade_lvl)::NUMERIC,1) AS upgrade_lvl_avg
FROM fin
GROUP BY 2
ORDER BY 2