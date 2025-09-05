/*
Ordem de execução de uma consulta:
1- FROM
2- JOIN
3- WHERE
4- GROUP BY 
5- HAVING
6- SELECT 
7- ORDER BY
8- LIMIT
*/

select
	player_name,
	pts,

	-- trata a coluna inteira de pontos como uma partição só
	-- rank pula uma posição em caso de empate
	rank() over(order by pts desc) as rank
from 
	player_seasons

-- 'from' e 'where' executam antes do 'select'
-- a coluna 'rank' não existe, esse where dá erro
-- where rank <= 10
order by rank;

-- usando CTE para resolver o problema acima
-- é a mesma coisa que usar subconsulta, mas é BEM mais legível
-- CTE é como uma variável que aponta para uma subconsulta
-- CADA vez que a CTE é referenciada, a consulta dela é processada
-- CTE não possui dados materializados
with ranked_players as (
	select
		player_name,
		pts,
		rank() over(order by pts desc) as rank
	from 
		player_seasons
)
select * from ranked_players
where rank <= 10
order by rank;

-- usando subconsulta
select * from (
	select
		player_name,
		pts,
		rank() over(order by pts desc) as rank
	from 
		player_seasons
) sub
where rank <= 10
order by rank;

-- materializando uma consulta (tabelas)
/*
Em um DataLake essa tabela não possui particionamento em nenhuma coluna,
o que é bem ruim na leitura, pois TODOS os dados serão lidos.
No BigQuery por exemplo o ideal seria existir uma coluna propícia para
particionar esses dados, geralmente por data.
*/
create table vmm_players_ranked as
select
	player_name,
	pts,
	rank() over(order by pts desc) as rank
from 
	player_seasons;

-- criando uma tabela particionável
-- aparentemente dá para particionar no postgres, mas parece ser meio chato
create table vmm_players_ranked_partitioned (
	player_name text,
	pts real,
	rank int,
	season int
) /* with (
	partitioning = array['season']
)
Particionamento, em DataLakes, nada mais é do que criar uma 
estrutura de pastas para cada tabela.
Quanto mais partições em colunas diferentes, mais subpastas
serão criadas.

Em DW's modernos (BigQuery, Redshift e Snowflake) o particionamento é
lógico.

Em DataLakes o particionamento é físico.
MAS, é recomendável uma camada lógica e não apenas
Parquet's "crus" para um melhor particionamento.
	- Tabelas Iceberg
	- Tabelas Delta
*/

truncate table vmm_players_ranked_partitioned 

-- o where dentro da cte rankeia apenas os dados de 2007
-- fora da cte ele pega um subconjunto dos dados já rankeados
/*
	"REGRA" em Engenharia de Dados:
		- Filtrar o mais cedo possível!
		
	Dessa forma eu processo a menor quantidade de dados possível.
	Nesse caso então a melhor abordagem é usar o where dentro
	da CTE (Principalmente se a tabela já estiver
	particionada por season, pois eu vou direto na pasta de 2007.
	O que não deve ser o caso aqui do Postgres.).
*/
insert into vmm_players_ranked_partitioned
with players_ranked as (
	select
		player_name,
		pts,
		rank() over(order by pts desc) as rank,
		season
	from 
		player_seasons
	where season = 2007
)
select * from players_ranked
--where season = 2007

select * from vmm_players_ranked_partitioned
where season = 2007 and rank = 1;