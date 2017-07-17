DECLARE @ids TABLE (id INT)
DECLARE @id INT

INSERT INTO @ids
EXEC dbo.Plog_start 'Testies'

SET @id = (SELECT TOP 1 * FROM @ids)

EXEC dbo.Plog_end @id
