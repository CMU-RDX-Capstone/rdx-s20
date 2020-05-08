-- top talkers for two weeks 
SELECT recon_id, COUNT(*) as incident_count
FROM   incidents
WHERE  submit_date_ms > 1567296000 -- start data here
	   AND submit_date_ms < 1568592000 -- end date here
GROUP BY recon_id
ORDER BY incident_count DESC
LIMIT 20;
