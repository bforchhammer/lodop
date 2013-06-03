-- Top Properties by #Statements

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

propGroups = GROUP quads BY predicate;
propStatements = FOREACH propGroups GENERATE group AS predicate, COUNT(quads) AS cnt;
propSorted = ORDER propStatements BY cnt DESC;
propSortedTop = FILTER propSorted BY cnt > 1;