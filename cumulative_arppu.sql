-- запрос на cumulative arppu когорты 
WITH pu AS ( 
    -- таблица игроков
  	SELECT  user_id,                 					-- id
            reg_dt::date  AS     reg_dt,             	-- reg dt
            count(user_id)  over() AS users_total		-- количество игроков в когорте
    FROM agg.paying_users_ht uh
    WHERE  
    -- пользовательские фильтры для Redash
    		('All' = '{{Platform}}' OR uh.platform = '{{Platform}}')
            AND uh.reg_dt::DATE BETWEEN '{{Date range.start}}' AND '{{Date range.end}}'
            AND ('All' = '{{Version from}}' OR regexp_split_to_array(install_ver, '\.')::int[] >=
                                               regexp_split_to_array('{{Version from}}', '\.')::int[])
            AND ('All' = '{{Country}}' OR uh.country = '{{Country}}')
            AND uh.platform NOT IN ('osx', 'webgl', 'windows', 'linux')
  		),
, pays AS (
    -- платежи по дням
	SELECT server_dt::DATE - reg_dt			AS day_at, 		    -- день от регистрации
         SUM(usd_net)                     	AS net			    -- сумма платежей    
	FROM pu 
		LEFT JOIN agg.in_app_purchases_ht iap USING(user_id)	-- таблица платежей      	
	WHERE iap.server_dt::DATE - pu.reg_dt::DATE <= 30			-- первые 30 дней от реги
  	GROUP BY 1
		)
, generator AS (
    -- генерирует 30 значений для дней от реги
	SELECT gg.day_at
		, users_total
		, 0 AS net
	FROM (SELECT* FROM generate_series(0,30) day_at)gg CROSS JOIN pu
	GROUP BY 1,2
	)
-- итоговый запрос
SELECT day_at
	, SUM(p.net+g.net) OVER (ORDER BY day_at) / users_total as cumulative_arppu
FROM generator g LEFT JOIN pays p USING (day_at)
GROUP BY 1,p.net,g.net, users_total
ORDER BY 1,2
