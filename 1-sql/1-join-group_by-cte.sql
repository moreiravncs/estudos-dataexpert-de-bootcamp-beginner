-- números de cada jogador por temporada
select * from player_seasons;

-- números de cada jogador por jogo
select * from game_details;

-- números de cada jogo
select * from games;

-- GROUP BY
-- lembrar que no group by. só o que pode estar no select são ou funções agregadoras ou as colunas usadas no agrupamento
select
	country,
	count(*),
	sum(reb),
	avg(pts),

	-- transforma todos os valores do grupo, para essa coluna, em um array
	-- distinct para evitar duplicados
	array_agg(distinct player_name)
from player_seasons
group by 1 -- primeira coluna do select
;

-- JOIN + GROUP BY
-- filtrando por apenas um jogador e usando array_agg eu posso checar se há duplicados com mais facilidade
select
	g.season,
	gd.player_name,
	sum(gd.pts) as total_points,
	count(*) as num_games,
	array_agg(g.game_date_est)
	-- gd.* -- todas as colunas de game_details
from 
	game_details gd join games g 
	on gd.game_id = g.game_id
where
	gd.player_name = 'LeBron James'
group by 
	g.season, gd.player_name -- todas as combinações diferentes dessas 2 colunas
order by gd.player_name
;

-- JOIN + GROUP BY + CTE
-- exemplo de deduplicação de registros (mas essa tabela não estava com dados duplicados, apenas a que ele mostrou no vídeo...)
with deduped_details as (
 	select distinct
 		player_name,
		game_id, 
		max(pts) as pts -- assumindo que essa coluna também está duplicada
 	from 
	    game_details
	group by 1, 2
),
deduped_games as (
 	select distinct
	 	game_id,
 		season,
		game_date_est
 	from 
	    games
	where 
		-- filtrando os jogos até a data inicial, mais ou menos, dos play-offs da nba
		game_date_est < date( cast(season + 1 as varchar) || '-04-15' )
)
select
	g.season,
	gd.player_name,
	sum(gd.pts) as total_points,
	count(*) as num_games,
	array_agg(distinct g.game_date_est)
	-- gd.* -- todas as colunas de game_details
from 
	deduped_details gd join deduped_games g 
	on gd.game_id = g.game_id
where
	gd.player_name = 'LeBron James'
group by 
	g.season, gd.player_name -- todas as combinações diferentes dessas 2 colunas
;