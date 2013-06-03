-- Top Properties by #URLs

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

urlPredAll = FOREACH quads GENERATE graph, predicate;
urlPred = DISTINCT urlPredAll;
propGroups = GROUP urlPred BY predicate;
propStatements = FOREACH propGroups GENERATE group AS predicate, COUNT(urlPred) AS cnt;
propSorted = ORDER propStatements BY cnt DESC;
propSortedTop = FILTER propSorted BY cnt > 1;