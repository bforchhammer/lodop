-- Property Co-Occurence By #URLs

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

groups = GROUP quads BY graph;
 
groupsCopy = FOREACH groups GENERATE FLATTEN(quads.predicate) AS p1, FLATTEN(quads.predicate) AS p2;
propertyCooc = FILTER groupsCopy BY p1 < p2;
propertyCoocGroup = GROUP propertyCooc BY *;
propertyCoocCounts = FOREACH propertyCoocGroup GENERATE FLATTEN(group) AS (p1,p2), COUNT(propertyCooc) AS cnt;
propertyCoocSorted = ORDER propertyCoocCounts BY p1, cnt DESC;
propertyCoocSortedTop = FILTER propertyCoocSorted BY cnt > 1;