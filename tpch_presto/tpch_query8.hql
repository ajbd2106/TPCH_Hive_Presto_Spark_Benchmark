CREATE TABLE tpch_query8_result AS

SELECT o_year
	,sum(CASE 
			WHEN nation = 'PERU'
				THEN volume
			ELSE 0
			END) / sum(volume) AS mkt_share
FROM (
	SELECT o_orderdate AS o_year
		,l_extendedprice * (1 - l_discount) AS volume
		,n2.n_name AS nation
	FROM part
		,lineitem
		,orders
		,customer
		,nation n1
		,nation n2
		,region
		,supplier
	WHERE p_partkey = l_partkey
		AND s_suppkey = l_suppkey
		AND l_orderkey = o_orderkey
		AND o_custkey = c_custkey
		AND c_nationkey = n1.n_nationkey
		AND n1.n_regionkey = r_regionkey
		AND r_name = 'AMERICA'
		AND s_nationkey = n2.n_nationkey
		AND o_orderdate BETWEEN '1995-01-01'
			AND '1996-12-31'
		AND p_type = 'ECONOMY BURNISHED NICKEL'
	) AS all_nations
GROUP BY o_year
ORDER BY o_year;