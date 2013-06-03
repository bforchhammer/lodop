-- Top Properties by #Pay Level Domains

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

pldPredAll = FOREACH quads GENERATE type,de.wbsg.loddesc.functions.PayLevelDomain(graph) AS domain, predicate;
pldPred = DISTINCT pldPredAll;
propGroups = GROUP pldPred BY predicate;
propStatements = FOREACH propGroups GENERATE group AS predicate, COUNT(pldPred) AS cnt;
propSorted = ORDER propStatements BY cnt DESC;
propSortedTop = FILTER propSorted BY cnt > 1;