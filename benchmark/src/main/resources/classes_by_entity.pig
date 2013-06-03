-- Top Classes by Entity

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

typedstuff = FILTER quads BY predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
classes = GROUP typedstuff BY object.value;
classCounts = FOREACH classes GENERATE group, COUNT(typedstuff) AS cnt;
topClassesByEntities = ORDER classCounts BY cnt DESC;
