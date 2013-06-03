-- Top Classes by Top Level Domain

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

typedstuff = FILTER quads BY predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
classesAndDomainsA = FOREACH typedstuff GENERATE object.value AS type,de.wbsg.loddesc.functions.PayLevelDomain(graph) AS domain;
classesAndDomains = DISTINCT classesAndDomainsA;
tlds = GROUP classesAndDomains BY (de.wbsg.loddesc.functions.TopLevelDomain(domain),type);
tldTypeCounts = FOREACH tlds GENERATE FLATTEN(group) AS (tld,type), COUNT(classesAndDomains) AS cnt;
topClassesTld = ORDER tldTypeCounts BY tld, cnt DESC;