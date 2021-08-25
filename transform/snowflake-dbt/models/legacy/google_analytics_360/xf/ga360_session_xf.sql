WITH sessions AS (

	SELECT *
	FROM {{ ref('ga360_session') }}

), 

sessions_xf AS(

	SELECT *	    
	FROM sessions		    
)

SELECT *
FROM sessions_xf
