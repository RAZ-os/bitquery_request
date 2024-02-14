DROP TABLE if exists public.tokens_quotation;

CREATE TABLE public.tokens_quotation (
	trade_start_date date NULL,
	trade_end_date date NULL,
	week_number int4 NULL,
	network text NULL,
	trade_operations_on text NULL,
	count_ int8 NULL,
	address text NULL,
	symbol text NULL,
	name text NULL,
	quoteamount float8 NULL,
	baseamount float8 NULL,
	oper_date timestamp NULL
);

drop function if exists tokens_insert;

CREATE OR REPLACE FUNCTION tokens_insert(a_start_date timestamp, a_end_date timestamp, a_json_data text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$	

BEGIN
INSERT INTO public.tokens_quotation
  (trade_start_date, trade_end_date, week_number, network, trade_operations_on, count_, address, symbol, "name", quoteamount, baseamount, oper_date)
SELECT date(a_start_date), date(a_end_date), DATE_PART('week', date(a_start_date))::int as week_number,
  (json_object_keys(js_array.jsdata::json)) as network,
  (json_object_keys(js_array.jsdata::json->(json_object_keys(js_array.jsdata::json)))) as trade_operations_on,
  dexTrades."count"  as count_, dexTrades."baseCurrency"->>'address' as address,
  dexTrades."baseCurrency"->>'symbol' as symbol, dexTrades."baseCurrency"->>'name' as name,
  dexTrades."quoteAmount" as quoteAmount, dexTrades."baseAmount" as baseAmount,
  localtimestamp as oper_date
FROM (
SELECT
a_json_data::json->'data' as jsdata
) js_array
LEFT JOIN LATERAL json_to_recordset((js_array.jsdata -> 'ethereum') -> 'dexTrades') dexTrades("count" bigint,
"baseCurrency" json, "quoteAmount" float, "baseAmount" float) ON true;
END; $$;

-- Чистим табличку
truncate table tokens_quotation;
delete from tokens_quotation where week_number = 3;

-- Достаём данные
select * from tokens_quotation tq
where tq.week_number = 4
and tq."name"  not in (select "name"  from tokens_quotation tq
where tq.week_number = 3 )

-- Достаём данные красивее
with level_1 as (
select * from tokens_quotation tq
)
select * from level_1 l1
where l1."name" not in (select tq."name" from tokens_quotation tq
where tq.week_number < l1.week_number);

select localtimestamp; 

