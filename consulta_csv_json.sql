select 
mercado_financeiro_json.id_market as "json",
mercado_financeiro_csv.id_market as "csv",
mercado_financeiro_json.date as "json",
mercado_financeiro_csv.date as "csv",
mercado_financeiro_json.low as "json",
mercado_financeiro_csv.low as "csv"
from mercado_financeiro_json
LEFT JOIN mercado_financeiro_csv 
	ON mercado_financeiro_csv.id = mercado_financeiro_json.id
;