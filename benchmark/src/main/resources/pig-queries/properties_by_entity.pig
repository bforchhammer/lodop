-- Top Properties by #Entities

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

subjPredAll = FOREACH quads GENERATE subject, predicate;
subjPred = DISTINCT subjPredAll;
propGroups = GROUP subjPred BY predicate;
propStatements = FOREACH propGroups GENERATE group AS predicate, COUNT(subjPred) AS cnt;
propSorted = ORDER propStatements BY cnt DESC;
propSortedTop = FILTER propSorted BY cnt > 1;