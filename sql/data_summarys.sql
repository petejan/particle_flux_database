SELECT file_id, count(d.file_id) AS cnt, f.number_samples from file f LEFT join data d using (file_id) WHERE d.sample_id = 0 or d.sample_id ISNULL group by f.file_id;

SELECT * FROM data where file_id = 100201 ORDER BY sample_id;

SELECT * FROM file_metadata fm WHERE file_id = 100201;

SELECT * FROM file fm WHERE file_id = 100201;

SELECT value, count(*) from file_metadata fm WHERE name like 'loginstatus' group by value

SELECT value, count(*) FROM file_metadata fm WHERE name LIKE 'event-1-device' GROUP BY value;