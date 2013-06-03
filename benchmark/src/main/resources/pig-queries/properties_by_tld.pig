-- Top Properties by #Top Level Domains

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

tldPredAll = FOREACH quads GENERATE de.wbsg.loddesc.functions.PayLevelDomain(graph) AS tld, predicate;
tldPred = DISTINCT tldPredAll;
tlds = GROUP tldPred BY (de.wbsg.loddesc.functions.TopLevelDomain(tld),predicate);
tldPredCounts = FOREACH tlds GENERATE FLATTEN(group) AS (tld,predicate), COUNT(tldPred) AS cnt;
topPredicatesTld = ORDER tldPredCounts BY tld, cnt DESC;