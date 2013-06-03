-- Top Classes by Pay Level Domain

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

typedstuff = FILTER quads BY predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
classesAndDomainsA = FOREACH typedstuff GENERATE object.value AS class,de.wbsg.loddesc.functions.PayLevelDomain(graph) AS domain;
classesAndDomains = DISTINCT classesAndDomainsA;
classes = GROUP classesAndDomains by class;
classCounts = FOREACH classes GENERATE group, COUNT(classesAndDomains) AS cnt;
topClasses = ORDER classCounts BY cnt DESC;